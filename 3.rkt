#lang typed/racket 

(require typed/racket/base
         typed/racket/class
         typed/racket/async-channel)

;; Currently no type for Queue?
(define-type Queue Any) 

(require/typed data/queue 
               [make-queue (-> Queue)]
               [enqueue! (-> Queue Any Void)]
               [dequeue! (-> Queue Any)]
               [queue-empty? (-> Queue Boolean)]
               [queue-length (-> Queue Integer)])


;;;;Data wants to be structured and stateful 
;;;;Functions want to be functional and stateless
;;;;Computation wants to be fast, efficient, asynchronous and seamless


;;;TODO:
;;; 1. Split datapool data from datapool cpu threads 
;;; 2. Put everything in structs to make internal implementation less brittle
;;; 3. Implement coroutine scheduler thread (sends signal to thread running oldest coroutine to (yield) when there are tasks waiting in the queues and no new task has been taken from a queue in x milli)
;;; 4. Improve (go) efficiency 
;;; 5. Implement and improve error handling
;;; 6. Implement input argument santization
;;; 7. Possibly improve hash and/or queue efficiency
;;; 8. Implement improvements based on feedback
;;; 9. Convert to C library (requires custom code for several components not supported by out of the box C)

(provide 
  ;;;DATAPOOL 
  make-computepool ;start a computation pool of worker threads for use in one or more datapools
  make-datapool ;create a datapool of hashed data and hashed message callbacks
  get-computepool ;return computepool specified in datapool
  get-num-dp-threads 
  close-dp ;kill all threads and processes in the datapool 

  ;;;COMMUNICATION
  channel ;create an asynchronous channel for inter-thread communication
  ch-get ;get from a channel
  ch-put ;send into a channel

  ;;;COMPUTATION
  define-coroutine ;return a coroutine procedure. When invoked produces a suspended coroutine for use in (go)
  go ;place a coroutine in a queue to be executed by a thread 

  ;;;DATA
  register-data! ;register given object in the datapool
  delete-data! ;delete object and all callbacks pointing to it via its key
  get-data ;get whatever data is in the data hash at provided key
  set-data! ;redefine whatever data is in the data hash at the provided key to the provided value
  get-data-field ;get the value of a registered object's field
  set-data-field! ;set a registered object's field via its key 

  ;;;MESSAGING
  message ;create a message object with a given type and content payload
  message-type ;return a message's type
  message-content ;return a message's content payload 
  message-source ;return a message's source key
  register-message-handler ;register a coroutine procedure that is called when a message with corresponding message-type and source-key 
  data-changed-type ;get the message type used for a specific data being changed in the data hash, used for setting up message handlers when data changes
  send-message ;send a message object to registered message handlers

  ;;;TESTING
  test-section
  test-true?
  test-equal?
  test-fail
  test-pass
  print-test-report
  run-3-ut)




;;;----------------------------------------------------------------------------
;;; 3 Library Errors
;;;----------------------------------------------------------------------------
(define-struct (invalid-argument exn:fail:user) ())

(: raise-inv-arg (-> String Any Any))
(define (raise-inv-arg [e : String] [var : Any #f]) 
  (let ([o (open-output-string)])
    (if (not (equal? var #f))
        (fprintf o "ERROR Invalid Argument: Symbol: ~a; ~a" var e)
        (fprintf o "ERROR Invalid Argument: ~a" e))
    (raise (make-invalid-argument 
             (get-output-string o) 
             (current-continuation-marks)))))



;;;----------------------------------------------------------------------------
;;; Convenience
;;;----------------------------------------------------------------------------
(: get-n-items (-> (Listof Any) Integer (Listof Any)))
(define get-n-items
  (lambda ([lst : (Listof Any)] [num : Integer])
    (if (> num 0)
        (cons (car lst) (get-n-items (cdr lst) (- num 1)))
        '()))) ;'

;; api: (slice list first-index length) 
(: slice (-> (Listof Any) Integer Exact-Nonnegative-Integer (Listof Any)))
(define slice
  (lambda ([lst : (Listof Any)] [start : Integer] [count : Exact-Nonnegative-Integer])
    (if (> start 1)
        (slice (cdr lst) (- start 1) count)
        (get-n-items lst count))))



;;;----------------------------------------------------------------------------
;;; TESTING - functions
;;;---------------------------------------------------------------------------- 
;; To wait for user input on test failure 
;; (define *run-3-tests-wait-before-cont* #t) 

;; Return #t if the given identifier is defined, else #f
(define-syntax (defined? stx)
  (syntax-case stx ()
               [(_ id)
                (with-syntax ([v (identifier-binding #'id)])
                             #''v)])) 

;Global test result tallies 
(: *test-number* Exact-Nonnegative-Integer)
(define *test-number* 1)

(: *num-passes* Exact-Nonnegative-Integer)
(define *num-passes* 0)

(: *passed-tests* (Listof Any))
(define *passed-tests* '())

(: *num-fails* Exact-Nonnegative-Integer)
(define *num-fails* 0)

(: *failed-tests* (Listof Any))
(define *failed-tests* '())

(: *cur-test-section* String)
(define *cur-test-section* "")

(: *tests-started* Boolean)
(define *tests-started* #f)

;Print test divider
(: print-test-divider (-> String Any))
(define (print-test-divider [char : String])
  (for ([i 80])
       (printf "~a" char))
  (printf "\n"))

;Zero test result tallies 
(: reset-test-results (-> Any))
(define (reset-test-results)
  (set! *test-number* 1)
  (set! *num-passes* 0)
  (set! *passed-tests* (list))
  (set! *num-fails* 0)
  (set! *failed-tests* (list))
  (set! *cur-test-section* ""))

(define (get-test-num)
  (let ([num *test-number*])
    (set! *test-number* (add1 *test-number*))
    num))

;Append test results to the global tallies
(define (collate-test [pass-fail : (Pair Boolean String)])
  (if (car pass-fail)
      (let ([test-string (cdr pass-fail)])
        (set! *num-passes* (add1 *num-passes*))
        (set! *passed-tests* (append *passed-tests* (list test-string))))
      (let ([test-string (cdr pass-fail)])
        (set! *num-fails* (add1 *num-fails*))
        (set! *failed-tests* (append *failed-tests* (list test-string))))))

;; PUBLIC API
;Print final test results
(define (print-test-report)
  (printf "\nNumber of test passes: ~a\n" *num-passes*)
  (printf "Number of test failures: ~a\n\n" *num-fails*)
  (printf "Failed tests:\n")
  (for ([i *failed-tests*])
       (printf "\t~a" i))
  (reset-test-results))

;; PUBLIC API
;Designate & print current test section with description
(define (test-section [name : String] [print : Boolean #t])
  (when (not (string? name))
    (raise-inv-arg "name not a string" name))
  (when (not (boolean? print))
    (raise-inv-arg "print not a boolean" print))

  (when print
    (let ()
      (set! *cur-test-section* name)
      (if *tests-started*
          (let ()
            (print-test-report)
            (printf "\n\n"))
          (set! *tests-started* #t))
      (print-test-divider "#")
      (printf "### test-section ~a\n" name)
      (print-test-divider "#"))))

;; PUBLIC API
;; Return #t if the quoted form returns #t, else #f 
(define (test-true? description form [print-result : Boolean #t] [wait : Boolean #f])
  (when (not (string? description))
    (raise-inv-arg "description not a string" description))
  (when (not (boolean? print-result))
    (raise-inv-arg "print-result not a boolean" print-result))
  (when (not (boolean? wait))
    (raise-inv-arg "wait not a boolean" wait))

  (if (equal? form #t)
      (let ([o (open-output-string)])
        (fprintf o "pass TEST ~a ~a: #t\n" (get-test-num) description)
        (when print-result
          (printf (get-output-string o)))
        (let ([ret (cons #t (get-output-string o))])
          (collate-test ret)
          ret))
      (let ([o (open-output-string)])
        (fprintf o "FAIL TEST ~a ~a: ~a != #t\n" (get-test-num) description form)
        (when print-result
          (printf (get-output-string o))
          (when wait
            (let ()
              (printf "<enter to continue>")
              (read-line (current-input-port) 'any))))
        (let ([ret (cons #f (get-output-string o))])
          (collate-test ret)
          ret))))

;; PUBLIC API
;; Return #t if quoted forms return an equal value, else #f
(define (test-equal? description [form-a : Any] [form-b : Any] [print-result : Boolean #t] [wait : Boolean #f])
  (when (not (string? description))
    (raise-inv-arg "description not a string" description))
  (when (not (boolean? print-result))
    (raise-inv-arg "print-result not a boolean" print-result))
  (when (not (boolean? wait))
    (raise-inv-arg "wait not a boolean" wait))

  (if (equal? form-a form-b)
      (let ([o (open-output-string)])
        (fprintf o "pass TEST ~a ~a: ~a == ~a\n" (get-test-num) description form-a form-b)
        (when print-result
          (printf (get-output-string o)))
        (let ([ret (cons #t (get-output-string o))])
          (collate-test ret)
          ret))
      (let ([o (open-output-string)])
        (fprintf o "FAIL TEST ~a ~a: ~a != ~a\n" (get-test-num) description form-a form-b)
        (when print-result
          (printf (get-output-string o))
          (when wait
            (let ()
              (printf "<enter to continue>")
              (read-line (current-input-port) 'any))))
        (let ([ret (cons #f (get-output-string o))])
          (collate-test ret)
          ret))))


;; PUBLIC API
;; Custom test fail
(define (test-fail [description : String] [form : Any] [print-result : Boolean #t] [wait : Boolean #f])
  (when (not (string? description))
    (raise-inv-arg "description not a string" description))
  (when (not (boolean? print-result))
    (raise-inv-arg "print-result not a boolean" print-result))
  (when (not (boolean? wait))
    (raise-inv-arg "wait not a boolean" wait))

  (let ([o (open-output-string)])
    (fprintf o "FAIL TEST ~a ~a: ~a\n" (get-test-num) description form)
    (when print-result
      (printf (get-output-string o))
      (when wait
        (let ()
          (printf "<enter to continue>")
          (read-line (current-input-port) 'any))))
    (let ([ret (cons #f (get-output-string o))])
      (collate-test ret)
      ret)))


;; PUBLIC API
;; Custom test pass
(define (test-pass description [form : Any] [print-result : Boolean #t])
  (when (not (string? description))
    (raise-inv-arg "description not a string" description))
  (when (not (boolean? print-result))
    (raise-inv-arg "print-result not a boolean" print-result))

  (let ([o (open-output-string)])
    (fprintf o "pass TEST ~a ~a: ~a\n" (get-test-num) description form)
    (when print-result
      (printf (get-output-string o)))
    (let ([ret (cons #t (get-output-string o))])
      (collate-test ret)
      ret)))



;;;----------------------------------------------------------------------------
;;; COMPUTATION - coroutines
;;;----------------------------------------------------------------------------  ;; Coroutine definition
(define-type Coroutine (-> Any * Any)) 
(define-type Coroutine-Generator (-> Any * Coroutine))

(: make-generator (-> (-> Any * Any) Coroutine))
(define (make-generator [procedure : (-> Any * Any)])

  (define last-return values)

  (: last-value Any)
  (define last-value #f)

  (: status Symbol)
  (define status 'suspended)

  (: last-continuation (-> Any Any))
  (define (last-continuation _) 
    (let ([result (procedure yield)]) 
      (last-return result)))

  (: yield (-> Any Any))
  (define (yield [value : Any])
    (call/cc (lambda ([continuation : (-> Any Any)])
               (set! last-continuation continuation)
               (set! last-value value)
               (set! status 'suspended)
               (last-return value)))) 

  (: coroutine Coroutine)
  (define 
    coroutine
    (lambda args
      (call/cc (lambda (return)
                 ;(set! last-return return)
                 (cond ((null? args) (let ()
                                       (set! status 'dead)
                                       (last-continuation last-value)))
                       ((eq? (car args) 'coroutine?) 'coroutine)
                       ((eq? (car args) 'dead?) (eq? status 'dead))
                       ((eq? (car args) 'suspended?) (not (eq? status 'dead)))
                       ((eq? (car args) 'kill!) (set! status 'dead)))))))
                       ;(#t (apply last-continuation args)))))))
  coroutine)


;;Define a function that will return a suspended coroutine created from given args and body forms
(define-syntax (define-coroutine stx)
  (syntax-case stx ()
               ((_ (name . args) . body )
                #`(begin 
                    (: name Coroutine-Generator)
                    (define (name . args)
                      (make-generator
                        (lambda (#,(datum->syntax stx 'yield))
                          . body)))))))



;;;----------------------------------------------------------------------------
;;; COMMUNICATION - channels
;;;---------------------------------------------------------------------------- 
;;create an async channel, no size limit by default 
(define-type Async-Channel-Size-Union (U Exact-Positive-Integer False))

(: channel (-> Async-Channel-Size-Union (Async-Channelof Any)))
(define (channel [size : Async-Channel-Size-Union #f]) 
  (make-async-channel size))


;;channel get, blocks by default 
(: ch-get (-> (Async-Channelof Any) Boolean Any))
(define (ch-get [ch : (Async-Channelof Any)] [block : Boolean #t])
  (if block
      (async-channel-get ch)
      (async-channel-try-get ch)))


;;channel put 
(: ch-put (-> (Async-Channelof Any) Any Void))
(define (ch-put [ch : (Async-Channelof Any)] [item : Any])
  (async-channel-put ch item))



;;;----------------------------------------------------------------------------
;;; DATAPOOL
;;;---------------------------------------------------------------------------- 

;;create computepool environment of worker threads 
(define-type Computepool (Vector (Vector Exact-Nonnegative-Integer 
                                         Queue 
                                         Semaphore) 
                                 (Vector Thread
                                         Queue 
                                         Semaphore))) 
(: make-computepool (-> Exact-Nonnegative-Integer Computepool))
(define (make-computepool [num-threads : Exact-Nonnegative-Integer])
  (let ([ret : Computepool
             (vector 
               (vector ;datapool info vector 
                 num-threads
                 (make-queue) ;queue of threads waiting to invoke (go)
                 (make-semaphore 1)) ;sem for sleeping thread queue
               (make-vector ;threads, queues, and semaphores
                 num-threads))])
    (for ([i num-threads])
         (let ()
           (vector-set! 
             (vector-ref ret 1) 
             i
             (vector 
               (thread (thunk (dp-thread-start ret))) ;threads
               (make-queue) ;thread task queues
               (make-semaphore 1))))) ;thread task queue semaphores 
    ret))

;; Data type def
(define-type Data-Key (U Exact-Nonnegative-Integer False))
(define-type Data-Field (U False Symbol))

;; Return destination type defs
(define-type Return-List-Channel-Entry (List Keyword Async-ChannelTop))
(define-type Return-List-Data-Entry (List Keyword Data-Key Data-Field))
(define-type Return-List-Message-Entry (List Keyword Any Data-Key))
(define-type Return-List-Datapool-Entry (List Keyword Datapool Data-Key Data-Field))
(define-type Return-List-Entry-Union (U Return-List-Channel-Entry
                                        Return-List-Data-Entry
                                        Return-List-Message-Entry
                                        Return-List-Datapool-Entry))
(define-type Return-List (U False (Listof Return-List-Entry-Union)))

;; Input data type def
(define-type Input-List (U False (Listof (List Data-Key Data-Field))))
(define-type Input-Data (Listof Any))

;; Callback handlers type def 
(define-type Message-Callback-Handler (List Coroutine-Generator Input-List Return-List))
(define-type Message-Callback-Handlers (Listof Message-Callback-Handler))
(define-type Message-Callback-Handlers-Union (U False Message-Callback-Handlers))

;; Message Callback Hash type def 
(define-type Message-Callback-Hash 
             (Mutable-HashTable Data-Key 
                                (Mutable-HashTable Data-Key 
                                                   Message-Callback-Handlers)))

;; Data Hash type def
(define-type Data-Hash (Mutable-HashTable Data-Key Any))

;; Datapool type def
(define-type Datapool (Boxof (Vector Computepool
                                     (Vector Message-Callback-Hash
                                             Semaphore) 
                                     (Vector Data-Hash
                                             Semaphore
                                             Data-Key
                                             Queue))))

;;create datapool environment of hashed data and message callbacks 
(: make-datapool (-> Computepool Datapool))
(define (make-datapool [computepool : Computepool]) 
  (let* ([key-src : Data-Key 0]
         [ret : Datapool
              (box 
                (vector  
                  computepool
                  (vector ;message handler vector
                    (make-hash) ;hash table of lists of message handlers
                    (make-semaphore 1)) ;message handlers semaphore
                  (vector ;user data vector
                    (make-hash) ;hash table of data objects
                    (make-semaphore 1) ;data hash semaphore
                    key-src ;data-obj-key-src
                    (make-queue))))]) ;data-obj-free-key-q 
    ret)) ;return a 'by reference' symbol


;; Global for maximum datapool task queue size 
(: *MAX-DP-QUEUE-SIZE* Exact-Nonnegative-Integer)
(define *MAX-DP-QUEUE-SIZE* 255)


(: get-num-dp-threads-intern (-> Computepool Exact-Nonnegative-Integer))
(define (get-num-dp-threads-intern [computepool : Computepool]) 
  (vector-ref (vector-ref computepool 0) 0))


;; PUBLIC API
;; Return the internally specified computepool of worker threads
(: get-computepool (-> Datapool Computepool))
(define (get-computepool [env : Datapool])
  (vector-ref (unbox env) 0))


;; PUBLIC API
;; Return number of threads in the datapool 
(: get-num-dp-threads (-> Datapool Exact-Nonnegative-Integer))
(define (get-num-dp-threads [env : Datapool]) 
  (vector-ref (vector-ref (vector-ref (unbox env) 0) 0) 0))


;; Return queue of sleeping threads waiting to invoke (go) 
(: get-waiting-threads-queue (-> Computepool Queue))
(define (get-waiting-threads-queue [computepool : Computepool]) 
  (vector-ref (vector-ref computepool 0) 1))


;; Return queue of sleeping threads waiting to invoke (go) 
(: get-waiting-threads-queue-sem (-> Computepool Semaphore))
(define (get-waiting-threads-queue-sem [computepool : Computepool]) 
  (vector-ref (vector-ref computepool 0) 2))


;; PUBLIC API
;;kill all threads and processes in a datapool 
(: close-dp (-> Datapool Void))
(define (close-dp [env : Datapool])
  ;kill threads
  (for ([i (get-num-dp-threads env)])
       (kill-thread (vector-ref 
                      (vector-ref 
                        (vector-ref 
                          (vector-ref 
                            (unbox env) 
                            0) 
                          1) 
                        i) 
                      0))))



;;;----------------------------------------------------------------------------
;;; COMPUTATION - thread management
;;;----------------------------------------------------------------------------
;;;threads
;; Get a thread pid at provided index 
(: get-dp-thread (-> Computepool Exact-Nonnegative-Integer Thread))
(define (get-dp-thread [computepool : Computepool] [idx : Exact-Nonnegative-Integer])
  (vector-ref 
    (vector-ref 
      (vector-ref 
        computepool
        1) 
      idx) 
    0))


;; Get a thread task queue at provided index 
(: get-dp-queue (-> Computepool Exact-Nonnegative-Integer Queue))
(define (get-dp-queue [computepool : Computepool] [idx : Exact-Nonnegative-Integer])
  (vector-ref 
    (vector-ref 
      (vector-ref 
        computepool
        1) 
      idx) 
    1))


;; Get a thread task queue semaphore at provided index 
(: get-dp-queue-sem (-> Computepool Exact-Nonnegative-Integer Semaphore))
(define (get-dp-queue-sem [computepool : Computepool] [idx : Exact-Nonnegative-Integer])
  (vector-ref 
    (vector-ref 
      (vector-ref 
        computepool
        1) 
      idx) 
    2))


;; Get the index of the fullest thread task queue 
(: get-max-dp-q-idx (-> Computepool Exact-Nonnegative-Integer))
(define (get-max-dp-q-idx [computepool : Computepool])
  (: longest Pair)
  (define longest (cons (queue-length (get-dp-queue computepool 0)) 0))
  (when (> (get-num-dp-threads-intern computepool) 1)
    (for ([i (in-range 1 (get-num-dp-threads-intern computepool))])
         (let ([cur-q-len (queue-length (get-dp-queue computepool i))])
           (when (< (car longest) cur-q-len)
             (begin
               (set! longest (cons cur-q-len i)))))))
  (cdr longest))


;; Get the index of the emptiest thread task queue
(: get-min-dp-q-idx (-> Computepool Exact-Nonnegative-Integer))
(define (get-min-dp-q-idx [computepool : Computepool])
  (define shortest (cons (queue-length (get-dp-queue computepool 0)) 0))
  (when (> (get-num-dp-threads-intern computepool) 1)
    (for ([i (in-range 1 (get-num-dp-threads-intern computepool))])
         (let ([cur-q-len (queue-length (get-dp-queue computepool i))])
           (when (> (car shortest) cur-q-len)
             (begin
               (set! shortest (cons cur-q-len i)))))))
  (cdr shortest))


;; PUBLIC API
;; Enqueues coroutine instance to the emptiest thread queue and resumes the 
;; thread. return-key-field-list is an optional argument where the user can 
;; specify a list of grouped keywords and arguments to determine how the 
;; returned values are stored.
;;
;; Example, if example-coroutine returns 2 values you can store them in the 
;; datapool environment thus:
;; (go env example-coroutine (list (list '#:data key1 'field1)
;;                                 (list '#:data key2 'field2)))
;;
;;
;; return-destinations accepts the following keywords with argument groups:
;; '#:message type source
;; '#:channel channel
;; '#:data key field ;if field is #f then the data at the provided key will be 
;;                   ;overwritten with the return value rather than modifying an 
;;                   ;object's field 
;; '#:datapool datapool key field ;same as '#:data but for the specified 
;;                                ;datapool data
;;
;; The actual values provided for each message/data/data-field/channel handler 
;; are the list of return values from the (go) function handled in the order 
;; received. If a return value is equal to '#:invalid, then that value's
;; destination handler is skipped.
;;
;; Valid return-destinations examples:
;; (list (list '#:data key field) (list '#:message type source))
;; (list (list '#:channel ch1) (list '#:channel ch2))
;; (list (list '#:data key1 field1) 
;;       (list '#:data key2 field2) 
;;       (list '#:message type source))
(: go (-> Datapool Coroutine Return-List Boolean))
(define (go [env : Datapool] 
            [suspended-coroutine : Coroutine] 
            [return-destinations : Return-List #f])
  (when (not (procedure? suspended-coroutine))
    (raise-inv-arg "suspended-coroutine not a procedure" suspended-coroutine))
  (when (not (equal? (suspended-coroutine 'coroutine?) 'coroutine))
    (raise-inv-arg "suspended-coroutine not a coroutine" suspended-coroutine))

  (let ([computepool (get-computepool env)])
    (let ([q-idx (get-min-dp-q-idx computepool)])
      ;put current thread to sleep if not a datapool worker thread and trying to 
      ;add too many (go) invocations, wait till queue empties a bit so as to not 
      ;continuously increase the size of the queues (can massively slow down 
      ;program execution)
      (let ([self (current-thread)]
            [num-threads (get-num-dp-threads env)]
            [datapool-worker #f])
        (for ([i num-threads])
             (when (equal? self (get-dp-thread computepool i))
               (set! datapool-worker #t)))

        (when (not datapool-worker)
          (when (>= (queue-length (get-dp-queue computepool q-idx)) *MAX-DP-QUEUE-SIZE*)
            (semaphore-wait (get-waiting-threads-queue-sem computepool))
            (enqueue! (get-waiting-threads-queue computepool) self)
            (semaphore-post (get-waiting-threads-queue-sem computepool))
            (thread-suspend self))))
      (semaphore-wait (get-dp-queue-sem computepool q-idx))
      (enqueue! (get-dp-queue computepool q-idx) (list env
                                                       suspended-coroutine 
                                                       return-destinations))
      (semaphore-post (get-dp-queue-sem computepool q-idx))
      (thread-resume (get-dp-thread computepool q-idx))
      #t)))



;;;----------------------------------------------------------------------------
;;; DATA
;;;----------------------------------------------------------------------------
;; Get the hash of data objects 
(: get-data-hash (-> Datapool Data-Hash))
(define (get-data-hash [env : Datapool])
  (vector-ref (vector-ref (unbox env) 2) 0))


;; Get the data objects semaphore 
(: get-data-sem (-> Datapool Semaphore))
(define (get-data-sem [env : Datapool])
  (vector-ref (vector-ref (unbox env) 2) 1))  


;; Get the data object current new key source integer 
(: get-data-key-src (-> Datapool Data-Key))
(define (get-data-key-src [env : Datapool])
  (vector-ref (vector-ref (unbox env) 2) 2)) 


;; Set the data object current new key source integer 
(: set-data-key-src (-> Datapool Data-Key Void))
(define (set-data-key-src [env : Datapool] [val : Data-Key])
  (vector-set! (vector-ref (unbox env) 2) 2 val)) 


;; Get the queue of freed data keys 
(: get-data-free-key-q (-> Datapool Queue))
(define (get-data-free-key-q [env : Datapool])
  (vector-ref (vector-ref (unbox env) 2) 3))


;; Add a recycled data object key to the container queue 
(: add-free-dp-data-key (-> Datapool Data-Key Void))
(define (add-free-dp-data-key [env : Datapool] [key : Data-Key]) 
  (enqueue! (get-data-free-key-q env) key))


;; Generate a new data object hash key 
(: gen-dp-data-obj-key (-> Datapool Data-Key))
(define (gen-dp-data-obj-key [env : Datapool])
  (if (> (queue-length (get-data-free-key-q env)) 0) 
      (dequeue! (get-data-free-key-q env))
      (let ([ret (get-data-key-src env)])
        (set-data-key-src env (+ (get-data-key-src env) 1)) 
        ret)))


;; PUBLIC API
;; Get a data object reference from the hash 
(: get-data (-> Datapool Data-Key Any))
(define (get-data [env : Datapool] [key : Data-Key])
  (hash-ref (get-data-hash env) key 'not-found))


;; PUBLIC API
;; Get the value of a data object's field 
(: get-data-field (-> Datapool Data-Key Data-Field Any))
(define (get-data-field [env : Datapool] [key : Data-Key] [field : Data-Field])
  (let ([data (get-data env key)])
    (if (equal? data 'not-found)
        data
        (let 
          ([ret 'not-found])
          (semaphore-wait (get-data-sem env))
          (set! ret (dynamic-get-field field data))
          (semaphore-post (get-data-sem env))
          ret))))


;; PUBLIC API 
;; get the message type used for when a specific data key is changed in the 
;; data hash, used for setting up message handlers when data changes:
;;
;; (register-message-handler 
;;   example-env 
;;   example-handler 
;;   (data-changed-type example-key 'example-field)                  ;<== when we get this message 
;;   #f                                                              ;<== from this source
;;   (list (list example-input-key 'example-input-field))            ;<== give these data values (and the message) as input into example-handler
;;   (list (list '#:data ex-destination-key 'ex-destination-field))) ;<== use these to direct where the output from example-handler is stored  
(: data-changed-type (-> Data-Key Data-Field Symbol))
(define (data-changed-type [key : Data-Key] [field : Data-Field #f])
  (let ([o (open-output-string)])
    (if field 
        (let ()
          (fprintf o "data-~a-~a-changed" key field)
          (string->symbol (get-output-string o)))
        (let ()
          (fprintf o "data-~a-changed" key)
          (string->symbol (get-output-string o))))))


;; PUBLIC API
;; Redefine data object field) 
(: set-data-field! (-> Datapool Data-Key Data-Field Any Boolean))
(define (set-data-field! [env : Datapool] [key : Data-Key] [field : Data-Field] [val : Any])
  (let ([data (get-data env key)])
    (if (not (equal? data 'not-found))
        (let ()
          (semaphore-wait (get-data-sem env))
          (dynamic-set-field! field (get-data env key) val)
          (semaphore-post (get-data-sem env))
          (send-message env (message (data-changed-type key field) field))
          #t)
        #f)))


;; Hash data object at provided key 
(: hash-data! (-> Datapool Data-Key Any Boolean))
(define (hash-data! [env : Datapool] [data-key : Data-Key] [data : Any])
  (hash-set! (get-data-hash env) data-key data)
  #t)



;;;----------------------------------------------------------------------------
;;; COMMUNICATION - messaging
;;;---------------------------------------------------------------------------- 
;; message class 
(define-type Message (Object (field (src Data-Key) 
                                    (type Symbol) 
                                    (content Any))))
(define message%
  (class object% 
         (super-new)
         (init-field
           [src : Data-Key]
           [type : Symbol]
           [content : Any])))


;; PUBLIC API
;; Create a message object 
(: message (-> Symbol Any Data-Key Message))
(define (message [type : Symbol] [content : Any] [source-key : Data-Key #f])
  (make-object message% source-key type content)) 


;; PUBLIC API
;; Return the message's type 
(: message-type (-> Message Symbol))
(define (message-type [msg : Message])
  (get-field type msg))


;; PUBLIC API
;; Return the message's content
(: message-content (-> Message Any))
(define (message-content [msg : Message])
  (get-field content msg))


;; PUBLIC API
;; Return the message's content 
(: message-source (-> Message Data-Key))
(define (message-source [msg : Message])
  (get-field src msg))


;; Get the hash of message callback handlers 
(: get-dp-message-handler-hash (-> Datapool Message-Callback-Hash))
(define (get-dp-message-handler-hash [env : Datapool])
  (vector-ref (vector-ref (unbox env) 1) 0))


;; Get the message callback handlers semaphore 
(: get-dp-message-handler-hash-sem (-> Datapool Semaphore))
(define (get-dp-message-handler-hash-sem [env : Datapool])
  (vector-ref (vector-ref (unbox env) 1) 1))

;; Get message callback handlers for msg-type and src-key 
(: get-dp-message-handlers (-> Datapool 
                               Symbol 
                               Data-Key 
                               Message-Callback-Handlers-Union))
(define (get-dp-message-handlers [env : Datapool] 
                                 [msg-type : Symbol] 
                                 [src-key : Data-Key])
  (if (hash-ref (get-dp-message-handler-hash env) msg-type #f)
      (hash-ref (hash-ref (get-dp-message-handler-hash env) msg-type) src-key #f)
      #f))


;; Internal function to set datapool message callback handlers
;; Does not check for semaphores 
(: set-dp-message-handlers-intern! (-> Datapool
                                       Symbol
                                       Data-Key
                                       Message-Callback-Handlers
                                       Void))
(define (set-dp-message-handlers-intern! [env : Datapool] 
                                         [msg-type : Symbol] 
                                         [src-key : Data-Key] 
                                         [handlers : Message-Callback-Handlers])
  (: ret Boolean)
  (define ret #t)

  ;; If msg-type hash doesn't exist create it
  (set! ret (and 
              ret 
              (if (hash-ref 
                    (get-dp-message-handler-hash env) 
                    msg-type 
                    #f)
                  #t
                  (hash-set! 
                    (get-dp-message-handler-hash env) 
                    msg-type 
                    (make-hash)))))

  ;; If src-key hash doesn't exist create it
  (set! ret (and ret 
                 (if (hash-ref 
                       (hash-ref 
                         (get-dp-message-handler-hash env) 
                         msg-type) 
                       src-key 
                       #f)
                     #t
                     (hash-set! 
                       (hash-ref 
                         (get-dp-message-handler-hash env) 
                         msg-type) 
                       src-key 
                       (make-hash)))))

  ;; Set list of handlers
  (set! ret (and
              ret 
              (hash-set! 
                (hash-ref 
                  (get-dp-message-handler-hash env) 
                  msg-type) 
                src-key handlers))))


;; Set the message handlers list for msg-type and src-key to something new 
(: set-dp-message-handlers! (-> Datapool
                                Symbol
                                Data-Key
                                Message-Callback-Handlers
                                Boolean))
(define (set-dp-message-handlers! [env : Datapool] 
                                  [msg-type : Symbol] 
                                  [src-key : Data-Key] 
                                  [handlers : Message-Callback-Handlers]) 
  ;; Take message sem first because another message handler may be using the 
  ;; data object sem, which would cause us to lock up
  (semaphore-wait (get-dp-message-handler-hash-sem env))
  (semaphore-wait (get-data-sem env))

  (set-dp-message-handlers-intern! env msg-type src-key handlers)

  (semaphore-post (get-data-sem env))
  (semaphore-post (get-dp-message-handler-hash-sem env))
  #t)


;; PUBLIC API
;; Register a callback handler coroutine (the procedure itself, not a 
;; suspended invocation). This coroutine should accept 1 message argument. If 
;; input-data is specified the coroutine should also accept a list argument 
;; where the extracted data shall be passed to the coroutine.
;;
;; The handler will only execute if an incoming message is sent with a matching
;; src-key to the one specified here. By default this is #f, and will only match 
;; messages with a src-key equal to #f.
;;
;;
;; When the callback activates the data stored at each key/field pair specified 
;; in input-data list is retrieved from the data hash and passed as a list to 
;; the handler after the msg variable in the following fashion:
;; (go env (handler msg input) return-destinations)
;;
;; As seen above return-destinations are passed to the (go) form when the 
;; handler is invoked.
;;
;;
;; input-data should be a list of key and fields like the following:
;; (list (list key1 'field1) (list key2 'field2) ... (list keyn 'fieldn))
;;
;; If the data itself (instead of a data object's field) is desired, simply set 
;; the field to #f:
;; (list (list key1 #f))
;;
;; return-destinations follows a slightly different convention (see 
;; documentation for the (go) function).
;;
;; Example:
;; (register-message-handler 
;;   example-env 
;;   example-handler 
;;   (data-changed-type example-key 'example-field)                  ;<== when we get this message 
;;   #f                                                              ;<== from this source
;;   (list (list example-input-key 'example-input-field))            ;<== give these data values (and the message) as input into example-handler
;;   (list (list '#:data ex-destination-key 'ex-destination-field))) ;<== use these to direct where the output from example-handler is stored 
(: register-message-handler (-> Datapool
                                Coroutine
                                Symbol
                                Data-Key
                                Input-List
                                Return-List
                                Boolean))
(define (register-message-handler 
          [env : Datapool]
          [coroutine-procedure : Coroutine]
          [msg-type : Symbol]
          [src-key : Data-Key #f] 
          [input-data : Input-List #f]
          [return-destinations : Return-List #f])
  (let ([msg-handlers (get-dp-message-handlers env msg-type src-key)])
    (if msg-handlers
        (set-dp-message-handlers! 
          env 
          msg-type 
          src-key
          (append msg-handlers (list (list coroutine-procedure 
                                           input-data
                                           return-destinations))))
        (set-dp-message-handlers! 
          env 
          msg-type 
          src-key
          (list (list coroutine-procedure 
                      input-data
                      return-destinations))))
    #t))


;; return list of data obtained from datapool based on given key-field pairs 
(: get-input-data (-> Datapool Input-List Input-Data))
(define (get-input-data [env : Datapool] [input-key-field-list : Input-List])
  (map 
    (lambda (data-key-field-pair) 
      (if (list? data-key-field-pair)
          (let ([key (car data-key-field-pair)]
                [field (cadr data-key-field-pair)])
            (if field 
                (get-data-field env key field)
                (get-data env key)))
          '#:invalid))
    input-key-field-list))


;; coroutine to manage sending messages to connected handlers
(define-coroutine 
  (send-message-co [env : Datapool] [msg : Message] [src-key : Data-Key])
  (let ([handlers : Message-Callback-Handlers-Union (get-dp-message-handlers 
                                                      env
                                                      (message-type msg)
                                                      src-key)])
    (if handlers
        (let ()
          (for-each
            (lambda ([i : Message-Callback-Handler])
              (let ([handler : Coroutine-Generator (car i)]
                    [input-key-field-list : Input-List (cadr i)]
                    [return-key-field-list : Return-List (caddr i)])
                (if input-key-field-list 
                    ;if input-key-field-list exists, grab the data and pass to 
                    ;handler coroutine alongside the message
                    (go env 
                        (handler msg (get-input-data env input-key-field-list)) 
                        return-key-field-list)
                    ;otherwise just pass the message to the coroutine
                    (go env (handler msg) return-key-field-list))))
            handlers)
          #t)
        #f)))


;; PUBLIC API
;; Send a message to connected handlers in the current datapool 
(: send-message (-> Datapool Message Boolean))
(define (send-message [env : Datapool] [msg : Message])
  (go env (send-message-co env msg (message-source msg))))


;; PUBLIC API
;; Data destructor. Removes message handlers, removes the data from the data 
;; hash, and enqueues the now free data key onto a free floating key queue 
(: delete-data! (-> Datapool Data-Key Boolean))
(define (delete-data! [env : Datapool] [data-key : Data-Key]) 
  ; Return #t if input is an atom
  (define (atom? [x : Any])
    (and (not (null? x))
         (not (pair? x))))

  ; Return #t if element is in list tree 
  (: find-in-tree (-> Any Any Boolean))
  (define (find-in-tree [tree : Any] [element : Any])
    (if (atom? tree)
        (if (equal? element tree) 
            #t 
            #f)
        (if (pair? tree)
            (let ([left (car tree)]
                  [right (cdr tree)])
              (if (find-in-tree left element) 
                  #t 
                  (find-in-tree right element)))
            #f)))

  (let ([data (get-data env data-key)])
    (if (not (equal? data 'not-found)) ;if data exists
        (let ()
          ;Need to protect against all modifications against the data hash AND
          ;the data message hash
          (semaphore-wait (get-dp-message-handler-hash-sem env))
          (semaphore-wait (get-data-sem env))

          (hash-for-each 
            (get-dp-message-handler-hash env)
            (lambda (msg-type msg-type-hash)
              ;Remove any hash where data-key is the src-key 
              (hash-remove! msg-type-hash data-key)

              ;Remove any callback where input-data or return-destinations 
              ;references data-key
              (hash-for-each 
                msg-type-hash
                (lambda (src-key callbacks)
                  ;generate list of callbacks that don't include deleted key
                  (let ([filtered-callback-list
                          (filter 
                            (lambda (current-callback)
                              (not (find-in-tree 
                                     current-callback 
                                     data-key)))
                            callbacks)])
                    (let ()
                      (if (null? filtered-callback-list)
                          ;when filtered list is empty delete hash entry
                          (hash-remove! msg-type-hash src-key) 
                          ;replace callbacks with list with deleted key removed
                          (set-dp-message-handlers-intern! 
                            env 
                            msg-type 
                            src-key
                            filtered-callback-list))))))

              ;Delete message type entry at top level if child hash is empty
              (when (equal? (hash-count msg-type-hash) 0) 
                (hash-remove! (get-dp-message-handler-hash env) msg-type))))

          ;Remove the data from the hash
          (hash-remove! (get-data-hash env) data-key)
          (semaphore-post (get-data-sem env))
          (semaphore-post (get-dp-message-handler-hash-sem env))

          ;Add data key to internal free floating queue of available keys
          (add-free-dp-data-key env data-key)
          #t)
        #f))) 


;; PUBLIC API
;; Register new data object with the datapool and return the data's new key 
(: register-data! (-> Datapool Any Data-Key))
(define (register-data! [env : Datapool] [data : Any])
  (let* ([key (gen-dp-data-obj-key env)]
         [cur-data (get-data env key)]
         [ret #f])
    (semaphore-wait (get-data-sem env))
    (when (equal? cur-data 'not-found) ;don't hash if data exists
      (let ()
        (hash-data! env key data)
        (send-message env (message (data-changed-type key) key))
        (set! ret key)))
    (semaphore-post (get-data-sem env))
    ret))


;; PUBLIC API
;; Redefine data at key 
(: set-data! (-> Datapool Data-Key Any Boolean))
(define (set-data! [env : Datapool] [key : Data-Key] [value : Any])
  (if (equal? (get-data env key) 'not-found)
      #f
      (let ()
        (semaphore-wait (get-data-sem env))
        (hash-data! env key value)
        (semaphore-post (get-data-sem env))
        (send-message env (message (data-changed-type key) value))
        #t)))


;Handle (go) return values by sending them to the specified locations in the 
;return-dests list
;
;return-dests list accepts the following keywords with argument groups:
;'#:message type source
;'#:data key field
;'#:channel channel
;
;The actual values provided for each message/data/data-field/channel handler 
;are the list of return values from the (go) function, in order. 
;
;Valid return-destination-list examples:
;(list (list '#:data key field) (list '#:message type source))
;(list (list '#:channel ch1) (list '#:channel ch2))
;(list (list '#:data key1 field1) 
;      (list '#:data key2 field2) 
;      (list '#:message type source))
;
;If a return value is '#:invalid, then the specific destination handler is 
;skipped. 
(: handle-go-returns (-> Datapool Return-List Any Boolean))
(define (handle-go-returns [env : Datapool] [return-dests : Return-List] [return-vals : Any])
  ;If return-vals is a single atom put it in a list
  (when (not (list? return-vals))
    (set! return-vals (list return-vals)))

  ;Pop off the front of a list, returning the popped value and resulting list
  (define (pop-list [inp-list : List])
    (if (null? inp-list)
        inp-list
        (values (car inp-list) (cdr inp-list))))

  ;Handle any message returns
  (define (handle-return-message [type : Symbol] [content : Any] [src : Data-Key])
    (send-message env (message type content src)))

  ;Handle any data returns
  (define (handle-return-data [key : Data-Key] [field : Data-Field] [val : Any])
    (if field
        ;set object field at data key
        (set-data-field! env key field val)
        ;replace data at key
        (set-data! env key val)))

  ;Handle any alternative datapool returns
  (define (handle-return-datapool [return-env : Datapool] 
                                  [key : Data-Key] 
                                  [field : Data-Field] 
                                  [val : Any])
    (if field 
        ;set object field at data key
        (set-data-field! return-env key field val)
        ;replace data at key 
        (set-data! return-env key val)))

  ;Handle any channel returns
  (define (handle-return-channel [ch : Async-ChannelTop] [val : Any])
    (ch-put ch val))

  ;Attempt to do something with the provided return value and destination 
  ;context
  (define (handle-return [dest : Return-List-Entry-Union] [val : Any])
    (let ([type : Keyword (car dest)])
      (if (equal? val '#:invalid)
          (values) ;do nothing
          (case type
            ['#:message 
             (if (equal? (length dest) 3)
                 (let ([message-type : Symbol (cadr dest)]
                       [source : Data-Key (caddr dest)])
                   (handle-return-message message-type val source))
                 (error "Incorrect number of arguments in '#:message (go) handler" (length dest)))]
            ['#:data 
             (if (equal? (length dest) 3)
                 (let ([key : Data-Key (cadr dest)]
                       [field : Data-Field (caddr dest)])
                   (handle-return-data key field val))
                 (error "Incorrect number of arguments in '#:data (go) handler" (length dest)))]
            ['#:datapool 
             (if (equal? (length dest) 4)
                 (let ([return-env : Datapool (cadr dest)]
                       [key : Data-Key (caddr dest)]
                       [field : Data-Field (cadddr dest)])
                   (handle-return-datapool return-env key field val))
                 (error "Incorrect number of arguments in '#:data (go) handler" (length dest)))]
            ['#:channel 
             (if (equal? (length dest) 2)
                 (let ([ch (cadr dest)])
                   (handle-return-channel ch val))
                 (error "Incorrect number of arguments in '#:channel (go) handler" (length dest)))]
            [else (error "Invalid type in (go) return handler" type)]))))

  ;Get subset lists that can be handled (lowest common list length between
  ;return-dests and return-vals) and attempt to handle the return value
  (define (handle-go-returns-intern [return-dests : Return-List] [return-vals : Any])
    (if (equal? return-dests #f)
        #t
        (let ([vals (if (< (length return-dests) (length return-vals))
                        (slice return-vals 0 (length return-dests))
                        return-vals)]
              [dests (if (> (length return-dests) (length return-vals))
                         (slice return-dests 0 (length return-vals))
                         return-dests)])
          (for ([val vals]
                [dest dests])
               (handle-return dest val)))))

  (handle-go-returns-intern return-dests return-vals))



;;;----------------------------------------------------------------------------
;;; COMPUTATION - thread functions
;;;----------------------------------------------------------------------------

;; Test flag
(: sanity-debug Boolean)
(define sanity-debug #f)

;; Return thread's queue index if not empty, otherwise gets the index of the 
;; fullest queue. 
(: get-task-q-idx (-> Computepool 
                      Exact-Nonnegative-Integer 
                      Exact-Nonnegative-Integer))
(define (get-task-q-idx [computepool : Computepool] [thread-idx : Exact-Nonnegative-Integer])
  (let ([thread-queue (get-dp-queue computepool thread-idx)])
    (if (equal? (queue-length thread-queue) 0)
        (let ([highest-idx (get-max-dp-q-idx computepool)])
          (if (equal? (queue-length (get-dp-queue computepool highest-idx)) 0) 
              #f
              highest-idx))
        thread-idx)))


;; Return a task from a thread queue to execute
(define-type Datapool-Task (List Datapool Coroutine Return-List))
(: get-task (-> Computepool Exact-Nonnegative-Integer Datapool-Task))
(define (get-task [computepool : Computepool] [thread-idx : Exact-Nonnegative-Integer])
  (let ([q-idx (get-task-q-idx computepool thread-idx)])
    (if (equal? q-idx #f)
        #f 
        (let ()
          (semaphore-wait (get-dp-queue-sem computepool q-idx))
          (let ([ret 
                  (if (queue-empty? (get-dp-queue computepool q-idx))
                      #f
                      (dequeue! (get-dp-queue computepool q-idx)))])
            (semaphore-post (get-dp-queue-sem computepool q-idx))
            ret)))))

(: *DP-THREAD-CLEAN-DIE* Boolean)
(define *DP-THREAD-CLEAN-DIE* #f)

;; Evaluate given task, limits cpu starvation by limiting continuous 
;; evaluations in coroutines. If provided task is *not* a coroutine and/or 
;; that coroutine does not (yield) intelligently this may have no effect.
;; Returns: #t if task completed, #f if task not yet completed 
(: dp-thread-exec-task (-> Computepool 
                           Exact-Nonnegative-Integer 
                           Datapool-Task
                           Boolean))
(define (dp-thread-exec-task [computepool : Computepool] 
                             [thread-idx : Exact-Nonnegative-Integer]
                             [task : Datapool-Task])
  (let ([co-env (car task)]
        [co (cadr task)]
        [return-dests (caddr task)])
    (let ([return-vals (co)])
      (if (co 'dead?) ;check if coroutine is dead 
          (let () ;task completed 
            (when (and return-dests return-vals)
              (handle-go-returns co-env return-dests return-vals))
            #t)
          (let ()
            (go co-env co return-dests) ;place task at the back of a queue
            #f)))))


;; Eternal thread tail recursion of executing tasks 
(: dp-thread (-> Computepool Exact-Nonnegative-Integer #t))
(define (dp-thread [computepool : Computepool] 
                   [thread-idx : Exact-Nonnegative-Integer]) 
  (with-handlers 
    ([exn:fail? 
       (lambda (e)
         (fprintf 
           (current-error-port)
           "dp-thread got error, current task thrown out: ~a\n"
           e)
         (dp-thread computepool thread-idx))])
    (let ([task (get-task computepool thread-idx)])
      (if (or (not task) (equal? (car task) #f))
          (thread-suspend (current-thread))
          (let ()
            (dp-thread-exec-task ;execute the task we get
              computepool 
              thread-idx 
              task)))))

  ;; Attempt to restart threads waiting to enqueue a (go) task
  (when (and (> (queue-length (get-waiting-threads-queue computepool)) 0)
             (< (queue-length (get-dp-queue computepool (get-min-dp-q-idx computepool))) 
                *MAX-DP-QUEUE-SIZE*))
    (semaphore-wait (get-waiting-threads-queue-sem computepool))
    (let ([thread (dequeue! (get-waiting-threads-queue computepool))])
      (when (not (thread-dead? thread)) ;throw out any dead threads
        (if (not (thread-running? thread))
            (thread-resume thread) ;resume thread waiting to invoke (go)
            (enqueue! (get-waiting-threads-queue computepool))))) ;thread not ready to resume
    (semaphore-post (get-waiting-threads-queue-sem computepool)))
  (if *DP-THREAD-CLEAN-DIE*
      #t
      (dp-thread computepool thread-idx)))


;; Thread startup coroutine
(define (dp-thread-start [computepool : Computepool])
  (let ([id (current-thread)])
    (: thread-num Exact-Nonnegative-Integer)
    (define thread-num 0)
    (for ([i (get-num-dp-threads-intern computepool)])
         (when (equal? (get-dp-thread computepool i) id) 
           (set! thread-num i)))
    (dp-thread computepool thread-num)))



;;;---------------------------------------------------------------------------- 
;;; TESTING - 3 unit tests
;;;---------------------------------------------------------------------------- 
;; Print out the lengths of all datapool environment worker task queues
(define (print-queue-lens [env : Datapool])
  (printf "\n")
  (for ([i (get-num-dp-threads env)])
       (printf "len task q ~a: ~a; " i (queue-length (get-dp-queue (get-computepool env) i))))
  (printf "\n\n"))

;; Wait until total task queue lengths == 0
(define (wait-len [env : Datapool])
  (: cenv Computepool)
  (define cenv (get-computepool env))

  (: idxs (Listof Exact-Nonnegative-Integer))
  (define idxs (list))
  (for ([i (get-num-dp-threads env)])
       (set! idxs (append idxs (list i))))

  (: check-in-time Flonum)
  (define check-in-time (current-inexact-milliseconds))

  (: inner-loop (-> Datapool (Listof Exact-Nonnegative-Integer) Boolean))
  (define (inner-loop [env : Datapool] 
                      [idxs : (Listof Exact-Nonnegative-Integer)])
    (let ([lens (map (lambda ([idx : Exact-Nonnegative-Integer]) 
                       (queue-length (get-dp-queue cenv idx))) 
                     idxs)]
          [done #t])
      (for-each 
        (lambda ([len : Real]) 
          (when (> len 0) (set! done #f)))
        lens)
      (if done
          #t
          (let ()
            (when (> (- (current-inexact-milliseconds) check-in-time) 5000)
              (set! check-in-time (current-inexact-milliseconds))
              (print-queue-lens env)
              ;(printf " ~a; " (queue-length (get-dp-queue env i))))
              ;(set! sanity-debug #t)
              (printf "\nWaiting for all tasks to complete...\n"))
            (sleep 0.01)
            (inner-loop env idxs)))))
  (inner-loop env idxs)
  (print-queue-lens env)
  (set! sanity-debug #f))

(: pr Boolean)
(define pr #t)

(: wait Boolean)
(define wait #f)

(define-namespace-anchor a)
(define (run-3-ut [print-results : Boolean #t] [wait-on-fail : Boolean #f]) 
  (set! pr print-results)
  (set! wait wait-on-fail)
  (parameterize ([current-namespace (namespace-anchor->namespace a)])
    (load "3-ut.rkt")))
