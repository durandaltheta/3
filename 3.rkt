#lang racket
(require racket/base
         racket/class
         racket/async-channel
         racket/place
         data/queue)

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
  make-datapool ;start a datapool
  get-computepool ;return computepool specified in datapool
  get-num-dp-threads ;get number of worker threads available to datapool
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
  make-message ;create a message object with a given type and content payload
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
  wait-dp ;wait for task queues to be empty
  print-test-report
  run-3-ut)




;;;----------------------------------------------------------------------------
;;; 3 Library Errors
;;;----------------------------------------------------------------------------
(define-struct (invalid-argument exn:fail:user) ())

(define (raise-inv-arg e [var #f]) 
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
(define get-n-items
  (lambda (lst num)
    (if (> num 0)
        (cons (car lst) (get-n-items (cdr lst) (- num 1)))
        '()))) ;'

;; api: (slice list first-index length)
(define slice
  (lambda (lst start count)
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
(define *test-number* 1)
(define *num-passes* 0)
(define *passed-tests* '())
(define *num-fails* 0)
(define *failed-tests* '())
(define *cur-test-section* "")
(define *tests-started* #f)

;Print test divider
(define (print-test-divider char)
  (if (string? char)
      (let ()
        (for ([i 80])
             (printf "~a" char))
        (printf "\n"))
      (raise-inv-arg "not a string" char))) 

;Zero test result tallies
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
(define (collate-test pass-fail)
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
(define (test-section name [print #t])
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
(define (test-true? description form [print-result #t] [wait #f])
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
(define (test-equal? description form-a form-b [print-result #t] [wait #f])
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
(define (test-fail description form [print-result #t] [wait #f])
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
(define (test-pass description form [print-result #t])
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
;;;----------------------------------------------------------------------------  
;; Coroutine definition  
(define (make-generator procedure)
  (define last-return values)
  (define last-value #f)
  (define status 'suspended)
  (define (last-continuation _) 
    (let ([result (procedure yield)]) 
      (last-return result)))

  (define (yield value)
    (call/cc (lambda (continuation)
               (set! last-continuation continuation)
               (set! last-value value)
               (set! status 'suspended)
               (last-return value))))

  (lambda args
    (call/cc (lambda (return)
               (set! last-return return)
               (cond ((null? args) 
                      (let ()
                        (set! status 'dead)
                        (last-continuation last-value)))
                     ((eq? (car args) 'coroutine?) 'coroutine)
                     ((eq? (car args) 'status?) status)
                     ((eq? (car args) 'dead?) (eq? status 'dead))
                     ((eq? (car args) 'alive?) (not (eq? status 'dead)))
                     ((eq? (car args) 'kill!) (set! status 'dead))
                     (#t (apply last-continuation args)))))))


;;Define a function that will return a suspended coroutine created from given args and body forms
(define-syntax (define-coroutine stx)
  (syntax-case stx ()
               ((_ (name . args) . body )
                #`(define (name . args)
                    (make-generator
                      (lambda (#,(datum->syntax stx 'yield))
                        . body))))))



;;;----------------------------------------------------------------------------
;;; COMMUNICATION - channels
;;;---------------------------------------------------------------------------- 
;;create an async channel, no size limit by default
(define (channel [size #f]) (make-async-channel size))


;;channel get, blocks by default
(define (ch-get ch [block #t])
  (if block
      (async-channel-get ch)
      (async-channel-try-get ch)))


;;channel put
(define (ch-put ch item)
  (async-channel-put ch item))



;;;----------------------------------------------------------------------------
;;; DATAPOOL
;;;---------------------------------------------------------------------------- 
;;create computepool environment of worker threads
(define (make-computepool num-threads)
  (let ([ret (vector 
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


;;create datapool environment of hashed data and message callbacks
(define (make-datapool computepool) 
  (let* ([key-src 0]
         [ret
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
(define *MAX-DP-QUEUE-SIZE* 255)


(define (get-num-dp-threads-intern computepool) 
  (vector-ref (vector-ref computepool 0) 0))


;; Return the datapool's environment data
(define (unbox-dp-env env)
  (unbox env))


;; PUBLIC API
;; Return the internally specified computepool of worker threads
(define (get-computepool env)
  (vector-ref (unbox-dp-env env) 0))


;; PUBLIC API
;; Return number of threads in the datapool
(define (get-num-dp-threads env) 
  (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 0) 0) 0))


;; Return queue of sleeping threads waiting to invoke (go)
(define (get-waiting-threads-queue computepool) 
  (vector-ref (vector-ref computepool 0) 1))


;; Return queue of sleeping threads waiting to invoke (go)
(define (get-waiting-threads-queue-sem computepool) 
  (vector-ref (vector-ref computepool 0) 2))


;; PUBLIC API
;;kill all threads and processes in a datapool
(define (close-dp env [force-close #f])
  (when (not force-close)
    (wait-dp env))
  ;kill threads
  (for ([i (get-num-dp-threads env)])
       (kill-thread (vector-ref 
                      (vector-ref 
                        (vector-ref 
                          (vector-ref 
                            (unbox-dp-env env) 
                            0) 
                          1) 
                        i) 
                      0)))
  (void))



;;;----------------------------------------------------------------------------
;;; COMPUTATION - thread management
;;;----------------------------------------------------------------------------
;;;threads
;; Get a thread pid at provided index
(define (get-dp-thread computepool idx)
  (vector-ref 
    (vector-ref 
      (vector-ref 
        computepool
        1) 
      idx) 
    0))


;; Get a thread task queue at provided index
(define (get-dp-queue computepool idx)
  (vector-ref 
    (vector-ref 
      (vector-ref 
        computepool
        1) 
      idx) 
    1))


;; Get a thread task queue semaphore at provided index
(define (get-dp-queue-sem computepool idx)
  (vector-ref 
    (vector-ref 
      (vector-ref 
        computepool
        1) 
      idx) 
    2))


;; Get the index of the fullest thread task queue
(define (get-max-dp-q-idx computepool)
  (define longest (cons (queue-length (get-dp-queue computepool 0)) 0))
  (when (> (get-num-dp-threads-intern computepool) 1)
    (for ([i (in-range 1 (get-num-dp-threads-intern computepool))])
         (let ([cur-q-len (queue-length (get-dp-queue computepool i))])
           (when (< (car longest) cur-q-len)
             (begin
               (set! longest (cons cur-q-len i)))))))
  (cdr longest))


;; Get the index of the emptiest thread task queue
(define (get-min-dp-q-idx computepool)
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
(define (go env suspended-coroutine [return-destinations #f])
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
(define (get-data-hash env)
  (vector-ref (vector-ref (unbox-dp-env env) 2) 0))


;; Get the data objects semaphore
(define (get-data-sem env)
  (vector-ref (vector-ref (unbox-dp-env env) 2) 1))  


;; Get the data object current new key source integer
(define (get-data-key-src env)
  (vector-ref (vector-ref (unbox-dp-env env) 2) 2)) 


;; Set the data object current new key source integer
(define (set-data-key-src env val)
  (vector-set! (vector-ref (unbox-dp-env env) 2) 2 val)) 


;; Get the queue of freed data keys
(define (get-data-free-key-q env)
  (vector-ref (vector-ref (unbox-dp-env env) 2) 3))


;; Add a recycled data object key to the container queue
(define (add-free-dp-data-key env key) 
  (enqueue! (get-data-free-key-q env) key))


;; Generate a new data object hash key
(define (gen-dp-data-obj-key env) 
  (if (> (queue-length (get-data-free-key-q env)) 0) 
      (dequeue! (get-data-free-key-q env))
      (let ([ret (get-data-key-src env)])
        (set-data-key-src env (+ (get-data-key-src env) 1)) 
        ret)))


;; PUBLIC API
;; Get a data object reference from the hash
(define (get-data env key)
  (hash-ref (get-data-hash env) key 'not-found))


;; PUBLIC API
;; Get the value of a data object's field
(define (get-data-field env key field)
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
(define (data-changed-type key [field #f])
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
(define (set-data-field! env key field val)
  (let ([data (get-data env key)])
    (if (not (equal? data 'not-found))
        (let ()
          (semaphore-wait (get-data-sem env))
          (dynamic-set-field! field (get-data env key) val)
          (semaphore-post (get-data-sem env))
          (send-message env (make-message (data-changed-type key field) field))
          #t)
        #f)))


;; Hash data object at provided key
(define (hash-data! env data-key data)
  (hash-set! (get-data-hash env) data-key data)
  #t)



;;;----------------------------------------------------------------------------
;;; COMMUNICATION - messaging
;;;---------------------------------------------------------------------------- 
;; message class 
(define message%
  (class object% 
         (super-new)
         (init-field
           src
           type
           content)))


;; PUBLIC API
;; Create a message object 
(define (make-message type content [source-key #f])
  (make-object message% source-key type content)) 


;; PUBLIC API
;; Return the message's type
(define (message-type msg)
  (get-field type msg))


;; PUBLIC API
;; Return the message's content
(define (message-content msg)
  (get-field content msg))


;; PUBLIC API
;; Return the message's content
(define (message-source msg)
  (get-field src msg))


;; Get the hash of message callback handlers
(define (get-dp-message-handler-hash env)
  (vector-ref (vector-ref (unbox-dp-env env) 1) 0))


;; Get the message callback handlers semaphore
(define (get-dp-message-handler-hash-sem env)
  (vector-ref (vector-ref (unbox-dp-env env) 1) 1))


;; Get message callback handlers for msg-type and src-key
(define (get-dp-message-handlers env msg-type src-key)
  (if (hash-ref (get-dp-message-handler-hash env) msg-type #f)
      (hash-ref (hash-ref (get-dp-message-handler-hash env) msg-type) src-key #f)
      #f))


;; Does not check for semaphores
(define (set-dp-message-handlers-intern! env msg-type src-key handlers)
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
(define (set-dp-message-handlers! env msg-type src-key handlers) 
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
(define (register-message-handler 
          env 
          coroutine-procedure 
          msg-type 
          [src-key #f] 
          [input-data #f]
          [return-destinations #f])
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
(define (get-input-data env input-key-field-list)
  (map 
    (lambda (data-key-field-pair) 
      (let ([key (car data-key-field-pair)]
            [field (cadr data-key-field-pair)])
        (if field 
            (get-data-field env key field)
            (get-data env key))))
    input-key-field-list))


;; coroutine to manage sending messages to connected handlers
(define-coroutine 
  (send-message-co env msg src-key)
  ;(define (get-dp-message-handlers env msg-type src-key)
  (let ([handlers (get-dp-message-handlers 
                    env
                    (message-type msg)
                    src-key)])
    (if handlers
        (let ()
          (map
            (lambda (i)
              (let ([handler (car i)]
                    [input-key-field-list (cadr i)]
                    [return-key-field-list (caddr i)])
                (if input-key-field-list 
                    ;if input-key-field-list exists, grab the data and pass to 
                    ;handler coroutine alongside the message
                    (go 
                      env 
                      (handler msg (get-input-data env input-key-field-list)) 
                      return-key-field-list)
                    ;otherwise just pass the message to the coroutine
                    (go env (handler msg) return-key-field-list))))
            handlers)
          #t)
        #f)))


;; PUBLIC API
;; Send a message to connected handlers in the current datapool
(define (send-message env msg)
  (go env (send-message-co env msg (message-source msg))))


;; PUBLIC API
;; Data destructor. Removes message handlers, removes the data from the data 
;; hash, and enqueues the now free data key onto a free floating key queue
(define (delete-data! env data-key) 
  ; Return #t if input is an atom
  (define (atom? x)
    (and (not (null? x))
         (not (pair? x))))

  ; Return #t if element is in list tree
  (define (find-in-tree tree element)
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
(define (register-data! env data)
  (let* ([key (gen-dp-data-obj-key env)]
         [cur-data (get-data env key)]
         [ret #f])
    (semaphore-wait (get-data-sem env))
    (when (equal? cur-data 'not-found) ;don't hash if data exists
      (let ()
        (hash-data! env key data)
        (send-message env (make-message (data-changed-type key) key))
        (set! ret key)))
    (semaphore-post (get-data-sem env))
    ret))


;; PUBLIC API
;; Redefine data at key
(define (set-data! env key value)
  (if (equal? (get-data env key) 'not-found)
      'not-found
      (let ()
        (semaphore-wait (get-data-sem env))
        (hash-data! env key value)
        (semaphore-post (get-data-sem env))
        (send-message env (make-message (data-changed-type key) value))
        key)))


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
(define (handle-go-returns env return-dests return-vals)
  ;If return-vals is a single atom put it in a list
  (when (not (list? return-vals))
    (set! return-vals (list return-vals)))

  ;Pop off the front of a list, returning the popped value and resulting list
  (define (pop-list inp-list)
    (if (null? inp-list)
        inp-list
        (values (car inp-list) (cdr inp-list))))

  ;Handle any message returns
  (define (handle-return-message type content src)
    (send-message env (make-message type content src)))

  ;Handle any data returns
  (define (handle-return-data key field val)
    (if field
        ;set object field at data key
        (set-data-field! env key field val)
        ;replace data at key
        (set-data! env key val)))

  ;Handle any alternative datapool returns
  (define (handle-return-datapool return-env key field val)
    (printf "handle-return-datapool 1 key: ~a; field: ~a\n" key field)
    (if field 
        (let ()
          (printf "handle-return-datapool 2\n")
          ;set object field at data key
          (set-data-field! return-env key field val)
          )
        (let ()
          (printf "handle-return-datapool 3 val: ~a\n" val)
          ;replace data at key
          (define set-data-ret (set-data! return-env key val))
          (printf "handle-return-datapool 4 set-data-ret: ~a; get-data: ~a\n" set-data-ret (get-data return-env key))
          )))

  ;Handle any channel returns
  (define (handle-return-channel ch val)
    (ch-put ch val))

  ;Attempt to do something with the provided return value and destination 
  ;context
  (define (handle-return dest val)
    (let ([type (car dest)])
      (if (equal? val '#:invalid)
          (values) ;do nothing
          (case type
            ['#:message 
             (if (equal? (length dest) 3)
                 (let ([type (cadr dest)]
                       [source (caddr dest)])
                   (handle-return-message type val source))
                 (error "Incorrect number of arguments in '#:message (go) handler" (length dest)))]
            ['#:data 
             (if (equal? (length dest) 3)
                 (let ([key (cadr dest)]
                       [field (caddr dest)])
                   (handle-return-data key field val))
                 (error "Incorrect number of arguments in '#:data (go) handler" (length dest)))]
            ['#:datapool 
             (if (equal? (length dest) 4)
                 (let ([return-env (cadr dest)]
                       [key (caddr dest)]
                       [field (cadddr dest)])
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
  (define (handle-go-returns-intern return-dests return-vals)
    (let ([vals (if (< (length return-dests) (length return-vals))
                    (slice return-vals 0 (length return-dests))
                    return-vals)]
          [dests (if (> (length return-dests) (length return-vals))
                     (slice return-dests 0 (length return-vals))
                     return-dests)])
      (for ([val vals]
            [dest dests])
           (handle-return dest val))))

  (handle-go-returns-intern return-dests return-vals))



;;;----------------------------------------------------------------------------
;;; COMPUTATION - thread functions
;;;----------------------------------------------------------------------------

;; Test flag
(define sanity-debug #f)

;; Return thread's queue index if not empty, otherwise gets the index of the 
;; fullest queue.
(define (get-task-q-idx computepool thread-idx)
  (let ([thread-queue (get-dp-queue computepool thread-idx)])
    (if (equal? (queue-length thread-queue) 0)
        (let ([highest-idx (get-max-dp-q-idx computepool)])
          (if (equal? (queue-length (get-dp-queue computepool highest-idx)) 0) 
              #f
              highest-idx))
        thread-idx)))


;; Return a task from a thread queue to execute
(define (get-task computepool thread-idx)
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


;; Evaluate given task, limits cpu starvation by limiting continuous 
;; evaluations in coroutines. If provided task is *not* a coroutine and/or 
;; that coroutine does not (yield) intelligently this may have no effect.
;; Returns: #t if task completed, #f if task not yet completed
(define (dp-thread-exec-task computepool thread-idx task)
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
(define (dp-thread computepool thread-idx) 
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
  (dp-thread computepool thread-idx))


;; Thread startup coroutine
(define (dp-thread-start computepool)
  (let ([id (current-thread)])
    (define thread-num 0)
    (for ([i (get-num-dp-threads-intern computepool)])
         (when (equal? (get-dp-thread computepool i) id) 
           (set! thread-num i)))
    (dp-thread computepool thread-num)))



;;;---------------------------------------------------------------------------- 
;;; TESTING - 3 unit tests
;;;---------------------------------------------------------------------------- 
;; Print out the lengths of all datapool environment worker task queues
(define (print-queue-lens env)
  (printf "\n")
  (for ([i (get-num-dp-threads env)])
       (printf "len task q ~a: ~a; " i (queue-length (get-dp-queue (get-computepool env) i))))
  (printf "\n\n"))

;; Wait until total task queue lengths == 0
(define (wait-dp env [print #f])
  (define cenv (get-computepool env))
  (define idxs (list))
  (for ([i (get-num-dp-threads env)])
       (set! idxs (append idxs (list i))))
  (define check-in-time (current-inexact-milliseconds))
  (define (inner-loop env idxs)
    (let ([lens (map (lambda (idx) (queue-length (get-dp-queue cenv idx))) idxs)]
          [done #t])
      (for-each 
        (lambda (len) 
          (when (> len 0) (set! done #f)))
        lens)
      (when (not done)
        (let ()
          (when (> (- (current-inexact-milliseconds) check-in-time) 5000)
            (set! check-in-time (current-inexact-milliseconds))
            (when print
              (print-queue-lens env)
              ;(printf " ~a; " (queue-length (get-dp-queue env i))))
              ;(set! sanity-debug #t)
              (printf "\nWaiting for all tasks to complete...\n")))
          (sleep 0.01)
          (inner-loop env idxs)))))
  (inner-loop env idxs)
  (when print
    (print-queue-lens env))
  (set! sanity-debug #f))

(define pr #t)
(define wait #f)

(define-namespace-anchor a)
(define (run-3-ut [print-results #t] [wait-on-fail #f]) 
  (set! pr print-results)
  (set! wait wait-on-fail)
  (parameterize ([current-namespace (namespace-anchor->namespace a)])
    (load "3-ut.rkt")))
