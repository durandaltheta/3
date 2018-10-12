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
;;; 1. finish basic unit tests (including removed tests and feature tests)
;;; 2. investigate and implement error handling correctly
;;; 3. expand unit tests to test edge-case inputs
;;; 4. optimize: yield, datapool structure, threading

;;;NEXTGEN TODO:
;;; 1. Put everything in structs to make internal implementation less brittle
;;; 2. Implement coroutine scheduler thread (sends signal to thread running oldest coroutine to (yield) when there are tasks waiting in the queues and no new task has been taken from a queue in x milli)
;;; 3. Improve (go) efficiency
;;; 4. Possibly improve hash and/or queue efficiency
;;; 5. Implement improvements based on feedback
;;; 6. Convert to C library (requires custom code for several components not supported by out of the box C)

(provide 
  ;;;DATAPOOL
  datapool ;start a datapool
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
  define-message-handler ;register a coroutine procedure that is called when a message with corresponding message-type and source-key 
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
;;create datapool data 
(define (datapool num-threads) 
  (let* ([key-src 0]
         [ret
           (box 
             (vector  
               (vector ;datapool info vector 
                 num-threads
                 (make-queue) ;queue of threads waiting to invoke (go)
                 (make-semaphore 1)) ;sem for sleeping thread queue
               (make-vector ;threads, queues, and semaphores
                 num-threads)
               (vector ;message handler vector
                 (make-hash) ;hash table of lists of message handlers
                 (make-semaphore 1)) ;message handlers semaphore
               (vector ;user data vector
                 (make-hash) ;hash table of data objects
                 (make-semaphore 1) ;data hash semaphore
                 key-src ;data-obj-key-src
                 (make-queue))))]) ;data-obj-free-key-q 
    (for ([i num-threads])
         (let ()
           (vector-set! 
             (vector-ref (unbox ret) 1) 
             i
             (vector 
               (thread (thunk (dp-thread-start ret))) ;threads
               (make-queue) ;thread task queues
               (make-semaphore 1))))) ;thread task queue semaphores 
    ret)) ;return a 'by reference' symbol


;; Return the datapool's environment data
(define (unbox-dp-env env)
  (unbox env))

;; PUBLIC API
;; Return number of threads in the datapool
(define (get-num-dp-threads env) 
  (vector-ref (vector-ref (unbox-dp-env env) 0) 0))


;; Return queue of sleeping threads waiting to invoke (go)
(define (get-waiting-threads-queue env) 
  (vector-ref (vector-ref (unbox-dp-env env) 0) 1))


;; Return queue of sleeping threads waiting to invoke (go)
(define (get-waiting-threads-queue-sem env) 
  (vector-ref (vector-ref (unbox-dp-env env) 0) 2))


;; PUBLIC API
;;kill all threads and processes in a datapool
(define (close-dp env)
  ;kill threads
  (for ([i (get-num-dp-threads env)])
       (kill-thread (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 1) i) 0))))



;;;----------------------------------------------------------------------------
;;; COMPUTATION - thread management
;;;----------------------------------------------------------------------------
;;;threads
;; Get a thread pid at provided index
(define (get-dp-thread env idx)
  (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 1) idx) 0))


;; Get a thread task queue at provided index
(define (get-dp-queue env idx)
  (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 1) idx) 1))


;; Get a thread task queue semaphore at provided index
(define (get-dp-queue-sem env idx)
  (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 1) idx) 2))


;; Get the index of the fullest thread task queue
(define (get-max-dp-q-idx env)
  (define longest (cons (queue-length (get-dp-queue env 0)) 0))
  (when (> (get-num-dp-threads env) 1)
    (for ([i (in-range 1 (get-num-dp-threads env))])
         (let ([cur-q-len (queue-length (get-dp-queue env i))])
           (when (< (car longest) cur-q-len)
             (begin
               (set! longest (cons cur-q-len i)))))))
  (cdr longest))


;; Get the index of the emptiest thread task queue
(define (get-min-dp-q-idx env)
  (define shortest (cons (queue-length (get-dp-queue env 0)) 0))
  (when (> (get-num-dp-threads env) 1)
    (for ([i (in-range 1 (get-num-dp-threads env))])
         (let ([cur-q-len (queue-length (get-dp-queue env i))])
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

  (let ([q-idx (get-min-dp-q-idx env)])
    ;put current thread to sleep if not a datapool worker thread and trying to 
    ;add too many (go) invocations, wait till queue empties a bit so as to not 
    ;continuously increase the size of the queues (can massively slow down 
    ;program execution)
    (let ([self (current-thread)]
          [num-threads (get-num-dp-threads env)]
          [datapool-worker #f])
      (for ([i num-threads])
           (when (equal? self (get-dp-thread env i))
             (set! datapool-worker #t)))

      (when (not datapool-worker)
        (when (> (queue-length (get-dp-queue env q-idx)) 255)
          (semaphore-wait (get-waiting-threads-queue-sem env))
          (enqueue! (get-waiting-threads-queue env) self)
          (semaphore-post (get-waiting-threads-queue-sem env))
          (thread-suspend self))))
    (semaphore-wait (get-dp-queue-sem env q-idx))
    (enqueue! (get-dp-queue env q-idx) (list suspended-coroutine 
                                             return-destinations))
    (semaphore-post (get-dp-queue-sem env q-idx))
    (thread-resume (get-dp-thread env q-idx))
    #t))



;;;----------------------------------------------------------------------------
;;; DATA
;;;----------------------------------------------------------------------------
;; Get the hash of data objects
(define (get-data-hash env)
  (vector-ref (vector-ref (unbox-dp-env env) 3) 0))


;; Get the data objects semaphore
(define (get-data-sem env)
  (vector-ref (vector-ref (unbox-dp-env env) 3) 1))  


;; Get the data object current new key source integer
(define (get-data-key-src env)
  (vector-ref (vector-ref (unbox-dp-env env) 3) 2)) 


;; Set the data object current new key source integer
(define (set-data-key-src env val)
  (vector-set! (vector-ref (unbox-dp-env env) 3) 2 val)) 


;; Get the queue of freed data keys
(define (get-data-free-key-q env)
  (vector-ref (vector-ref (unbox-dp-env env) 3) 3))


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
;; (define-message-handler 
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
          (send-message env (message (data-changed-type key field) field))
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
(define (message type content [source-key #f])
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
  (vector-ref (vector-ref (unbox-dp-env env) 2) 0))


;; Get the message callback handlers semaphore
(define (get-dp-message-handler-hash-sem env)
  (vector-ref (vector-ref (unbox-dp-env env) 2) 1))


;; Get message callback handlers for msg-type and src-key
(define (get-dp-message-handlers env msg-type src-key)
  (if (hash-ref (get-dp-message-handler-hash env) msg-type #f)
      (hash-ref (hash-ref (get-dp-message-handler-hash env) msg-type) src-key #f)
      #f))


;; Set the message handlers list for msg-type and src-key to something new
(define (set-dp-message-handlers! env msg-type src-key handlers) 
  ;; Take message sem first because another message handler may be using the 
  ;; data object sem, which would cause us to lock up
  (semaphore-wait (get-dp-message-handler-hash-sem env))
  (semaphore-wait (get-data-sem env))
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
                src-key handlers)))
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
;; (define-message-handler 
;;   example-env 
;;   example-handler 
;;   (data-changed-type example-key 'example-field)                  ;<== when we get this message 
;;   #f                                                              ;<== from this source
;;   (list (list example-input-key 'example-input-field))            ;<== give these data values (and the message) as input into example-handler
;;   (list (list '#:data ex-destination-key 'ex-destination-field))) ;<== use these to direct where the output from example-handler is stored
(define (define-message-handler 
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


;; coroutine to manage sending messages to connected handlers
(define-coroutine 
  (send-message-co env msg src-key)
  (let ([handlers (get-dp-message-handlers 
                    env
                    (get-field type msg)
                    src-key)])
    (if handlers
        (let ()
          (map
            (lambda (i)
              (let ([handler (car i)]
                    [input-key-field-list (car (cdr i))]
                    [return-key-field-list (car (cddr i))])
                (go env (handler msg) return-key-field-list)))
            handlers)
          #t)
        #f)))


;; PUBLIC API
;; Send a message to connected handlers in the current datapool
(define (send-message env msg [src-key #f])
  (go env (send-message-co env msg src-key)))


;; PUBLIC API
;; Data destructor. Removes message handlers, removes the data from the data 
;; hash, and enqueues the now free data key onto a free floating key queue
(define (delete-data! env data-key) 
  (let ([data (get-data env data-key)])
    (if (not (equal? data 'not-found)) ;if data exists
        (let ()
          ;Need to protect against all modifications against the data hash AND
          ;the data message hash
          (semaphore-wait (get-dp-message-handler-hash-sem env))
          (semaphore-wait (get-data-sem env))

          ;Remove any hash where data-key is the src-key 
          (hash-for-each 
            (get-dp-message-handler-hash env)
            (lambda (msg-type cur-hash)
              (hash-remove! (hash-ref (get-dp-message-handler-hash env) msg-type) data-key)))

          ;Remove all callbacks to data-key 
          (hash-for-each 
            (get-dp-message-handler-hash env) 
            (lambda (msg-type msg-type-hash)
              (hash-remove! msg-type-hash data-key)
              (when
                (equal? (hash-count msg-type-hash) 0)
                (hash-remove! (get-dp-message-handler-hash env) msg-type))))

          ;Remove the data from the hash
          (hash-remove! (get-data-hash env) data-key)
          (semaphore-post (get-data-sem env))
          (semaphore-post (get-dp-message-handler-hash-sem env))

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
        (send-message env (message (data-changed-type key) key))
        (set! ret key)))
    (semaphore-post (get-data-sem env))
    ret))


;; PUBLIC API
;; Redefine data at key
(define (set-data! env key value)
  (if (equal? (get-data env key) 'not-found)
      (let ()
        (semaphore-wait (get-data-sem env))
        (hash-data! env key value)
        (semaphore-post (get-data-sem env))
        (send-message env (message (data-changed-type key) value))
        key)
      'not-found))


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
    (send-message env (message type content src)))

  ;Handle any data returns
  (define (handle-return-data key field val)
    (if field
        ;set object field at data key
        (set-data-field! env key field val)
        ;replace data at key
        (set-data! env key val)))

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
                   (handle-return-message type source))
                 (error "Incorrect number of arguments in '#:message (go) handler" (length dest)))]
            ['#:data 
             (if (equal? (length dest) 3)
                 (let ([key (cadr dest)]
                       [field (caddr dest)])
                   (handle-return-data key field val))
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
;;; threads
;; Return thread's queue index if not empty, otherwise gets the index of the 
;; fullest queue.
(define (get-task-q-idx env thread-idx)
  (let ([thread-queue (get-dp-queue env thread-idx)])
    (if (equal? (queue-length thread-queue) 0)
        (let ([highest-idx (get-max-dp-q-idx env)])
          (if (equal? (queue-length (get-dp-queue env highest-idx)) 0) 
              #f
              highest-idx))
        thread-idx)))


;; Return a task from a thread queue to execute
(define (get-task env thread-idx)
  (let ([q-idx (get-task-q-idx env thread-idx)])
    (if (equal? q-idx #f)
        #f 
        (let ()
          (semaphore-wait (get-dp-queue-sem env q-idx))
          (let ([ret 
                  (if (queue-empty? (get-dp-queue env q-idx))
                      #f
                      (dequeue! (get-dp-queue env q-idx)))])
            (semaphore-post (get-dp-queue-sem env q-idx))
            ret)))))


;; Evaluate given task, limits cpu starvation by limiting continuous 
;; evaluations in coroutines. If provided task is *not* a coroutine and/or 
;; that coroutine does not (yield) intelligently this may have no effect.
;; Returns: #t if task completed, #f if task not yet completed
(define (dp-thread-exec-task env thread-idx task)
  (let* ([co (car task)]
         [return-dests (cadr task)]
         [return-vals (co)])
    (if (co 'dead?) ;check if coroutine is dead 
        (let () ;task completed 
          (when (and return-dests return-vals)
            (handle-go-returns env return-dests return-vals))
          #t)
        (let ()
          (go env co return-dests) ;place task at the back of a queue
          #f))))


;; Eternal thread tail recursion of executing tasks
(define (dp-thread env thread-idx) 
  (with-handlers 
    ([exn:fail? 
       (lambda (e)
         (fprintf 
           (current-error-port)
           "dp-thread got error, current task thrown out: ~a\n"
           e)
         (dp-thread env thread-idx))])
    (let ([task (get-task env thread-idx)])
      (if (or (not task) (equal? (car task) #f))
          (thread-suspend (current-thread))
          (let ()
            (dp-thread-exec-task ;execute the task we get
              env 
              thread-idx 
              task)))))

  (when (> (queue-length (get-waiting-threads-queue env)) 0)
    (semaphore-wait (get-waiting-threads-queue-sem env))
    (let ([thread (dequeue! (get-waiting-threads-queue env))])
      (when (not (thread-dead? thread)) ;throw out any dead threads
        (if (not (thread-running? thread))
            (thread-resume thread) ;resume thread waiting to invoke (go)
            (enqueue! (get-waiting-threads-queue env))))) ;thread not ready to resume
    (semaphore-post (get-waiting-threads-queue-sem env)))
  (dp-thread env thread-idx))


;; Thread startup coroutine
(define (dp-thread-start env)
  (let ([id (current-thread)])
    (define thread-num 0)
    (for ([i (get-num-dp-threads env)])
         (when (equal? (get-dp-thread env i) id) 
           (set! thread-num i)))
    (dp-thread env thread-num)))



;;;---------------------------------------------------------------------------- 
;;; TESTING - 3 unit tests
;;;---------------------------------------------------------------------------- 
(define pr #t)
(define wait #f)

(define-namespace-anchor a)
(define (run-3-ut [print-results #t] [wait-on-fail #f]) 
  (set! pr print-results)
  (set! wait wait-on-fail)
  (parameterize ([current-namespace (namespace-anchor->namespace a)])
    (load "3-ut.rkt")))
