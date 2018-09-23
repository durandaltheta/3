#lang racket
(require racket/base
         racket/class
         racket/async-channel
         racket/place
         data/queue)

;;;;Data wants to be structured and stateful 
;;;;Functions want to be functional and stateless
;;;;Computation wants to be asynchronous and seamless

;;;TODO:
;;; 1. separate datapool into datapool and computepool
;;; 2. convert pools to structs
;;; 3. parallel place debugging
;;; 4. finish basic unit tests (including removed tests)
;;; 5. investigate and implement error handling correctly
;;; 6. expand unit tests to test edge-case inputs
;;; 7. optimize: yield, datapool/computepool structure, threading

(provide 
  ;;;DATAPOOL
  make-datapool ;start a datapool
  get-num-dp-threads 
  get-num-dp-proc-threads 
  close-dp ;kill all threads and processes in the datapool 

  ;;;COMMUNICATION
  channel ;create an asynchronous channel for inter-thread communication
  ch-get ;get from a channel
  ch-put ;send into a channel

  ;;;COMPUTATION
  define-coroutine ;return a coroutine procedure. When invoked produces a suspended coroutine for use in (go)
  go ;place a coroutine in a queue to be executed by a thread 
  go-proc ;send a quoted form to be evaluated by another process

  ;;;DATA
  register-data! ;register given object in the datapool
  delete-data! ;delete object and all callbacks pointing to it via its key
  get-data-field ;get the value of a registered object's field
  set-data-field! ;set a registered object's field via its key 

  ;;;MESSAGING
  message ;create a message object with a given type and content payload
  message-type ;return a message's type
  message-content ;return a message's payload 
  message-source ;return a message's source key
  register-message-handler ;register a coroutine procedure that is called when a message with corresponding message-type and source-key 
  send-message ;send a message object to registered message handlers

  ;;;TESTING
  test-section
  test-true?
  test-equal?
  test-fail
  test-pass
  print-test-report
  run-3-unit-tests)




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
        (read-line (current-input-port) 'any)))
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
  (if (place-channel? ch)
    (place-channel-get ch)
    (if block
      (async-channel-get ch)
      (async-channel-try-get ch))))


;;channel put
(define (ch-put ch item)
  (if (place-channel? ch)
    (place-channel-put ch item)
    (async-channel-put ch item)))



;;;----------------------------------------------------------------------------
;;; DATAPOOL
;;;----------------------------------------------------------------------------
;;create datapool data 
(define (make-datapool num-threads num-processes) 
  (let* ([key-src 0]
         [ret
           (box 
             (vector  
               (vector ;datapool info vector 
                 num-threads
                 num-processes)
               (make-vector ;threads, queues, and semaphores
                 num-threads)
               (make-vector ;processes, proc management threads, queues, and semaphores
                 num-processes)
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
    (for ([i num-processes])
         (let-values ([(local-channel process-channel) (place-channel)])
                     (vector-set! 
                       (vector-ref (unbox ret) 2) 
                       i
                       (vector 
                         (list local-channel process-channel) ;process-to-process communication channels
                         (place
                           process-channel
                           ;#lang racket 
                           (define (dp-process)
                             (with-handlers 
                               ([exn:fail? 
                                  (lambda (e)
                                    (let ([o (open-output-string)])
                                      (fprintf o "Got error: ~a\n" e)
                                      (place-channel-put 
                                        process-channel 
                                        'died-before-execution)
                                      (place-channel-put 
                                        process-channel 
                                        (get-output-string o))
                                      (dp-process)))])
                               (let* ([task-bundle (place-channel-get process-channel)]
                                      [quoted-task (car task-bundle)]
                                      [user-channel (car (cdr task-bundle))])
                                 (with-handlers 
                                   ([exn:fail? 
                                      (lambda (e)
                                        (let ([o (open-output-string)])
                                          (fprintf o "Got error: ~a\n" e)
                                          (place-channel-put 
                                            process-channel 
                                            'died-during-execution)
                                          (place-channel-put 
                                            process-channel 
                                            (get-output-string o))
                                          (dp-process)))])
                                   (if quoted-task
                                     (let ([ret (eval quoted-task (current-namespace))])
                                       (place-channel-put process-channel ret)
                                       (dp-process))
                                     #f))))) ;if no task end process
                           (dp-process))
                         (thread (thunk (dp-process-thread-start ret))) ;threads 
                         (make-queue) ;process thread task queues
                         (make-semaphore 1))))) ;thread task queue semaphores 
    ret)) ;return a 'by reference' symbol


;; Return the datapool's environment data
(define (unbox-dp-env env)
  (unbox env))

;; PUBLIC API
;; Return number of threads in the datapool
(define (get-num-dp-threads env) 
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))

  (vector-ref (vector-ref (unbox-dp-env env) 0) 0))

;; PUBLIC API
;; Return number of threads in the datapool
(define (get-num-dp-proc-threads env) 
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))

  (vector-ref (vector-ref (unbox-dp-env env) 0) 1))

;; PUBLIC API
;;kill all threads and processes in a datapool
(define (close-dp env)
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))

  ;kill threads
  (for ([i (get-num-dp-threads env)])
       (kill-thread (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 1) i) 0)))

  ;kill processes
  (for ([i (get-num-dp-proc-threads env)])
       (place-kill (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 2) i) 1))
       (kill-thread (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 2) i) 2))))



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
;;Enqueues coroutine instance to the emptiest thread queue and resumes the 
;;thread. Ret-key and ret-field are optional arguments that tell the thread 
;;what to do with the value returned at the end of coroutine execution.
;;
;;(if (and ret-key ret-field)) attempts to (send ret-field (unbox-dp-env env ret-key) return-value)
;;(if (and ret-key (not ret-field)) attempts to (set-dp-data! env ret-key return-value)
(define (go env suspended-coroutine [ret-key #f] [ret-field #f])
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))
  (when (and (not (boolean? ret-key)) (not (number? ret-key)))
    (raise-inv-arg "ret-key not a number or #f" ret-key))
  (when (not (procedure? suspended-coroutine))
    (raise-inv-arg "suspended-coroutine not a procedure" suspended-coroutine))
  (when (not (equal? (suspended-coroutine 'coroutine?) 'coroutine))
    (raise-inv-arg "suspended-coroutine not a coroutine" suspended-coroutine))

  (let ([q-idx (get-min-dp-q-idx env)])
    (semaphore-wait (get-dp-queue-sem env q-idx))
    (enqueue! (get-dp-queue env q-idx) (list suspended-coroutine ret-key ret-field))
    (semaphore-post (get-dp-queue-sem env q-idx))
    (thread-resume (get-dp-thread env q-idx))
    #t))



;;;processes (places)
;; Get the place channel for specified process index. Things put in the channel 
;; are visible to the associated process within its personal process-channel
(define (get-process-channel env proc-num)
  (car (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 2) proc-num) 0)))


(define (get-remote-process-channel env proc-num)
  (car (cdr (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 2) proc-num) 0))))


;; Get a place process pid at provided index
(define (get-dp-proc env idx)
  (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 2) idx) 1))


;; Get a process thread pid at provided index
(define (get-dp-proc-thread env idx)
  (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 2) idx) 2))


;; Get a thread task queue at provided index
(define (get-dp-proc-queue env idx)
  (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 2) idx) 3))


;; Get a thread task queue semaphore at provided index
(define (get-dp-proc-queue-sem env idx)
  (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 2) idx) 4))


;; Get the index of the fullest thread task queue
(define (get-max-dp-proc-q-idx env)
  (define longest (cons (queue-length (get-dp-proc-queue env 0)) 0))
  (when (> (get-num-dp-proc-threads env) 1)
    (for ([i (in-range 1 (get-num-dp-proc-threads env))])
         (let ([cur-q-len (queue-length (get-dp-proc-queue env i))])
           (when (< (car longest) cur-q-len)
             (begin
               (set! longest (cons cur-q-len i)))))))
  (cdr longest))


;; Get the index of the emptiest thread task queue
(define (get-min-dp-proc-q-idx env)
  (define shortest (cons (queue-length (get-dp-proc-queue env 0)) 0))
  (when (> (get-num-dp-proc-threads env) 1)
    (for ([i (in-range 1 (get-num-dp-proc-threads env))])
         (let ([cur-q-len (queue-length (get-dp-proc-queue env i))])
           (when (> (car shortest) cur-q-len)
             (begin
               (set! shortest (cons cur-q-len i)))))))
  (cdr shortest))


;; PUBLIC API
;;Enqueues quoted form for evaluation in another process. If ret-key and ret-field
;;are not specified no return values will be provided for the task. Additionally
;;messaging and message-handlers *DO NOT WORK* in the evaluation process. Communication
;;must then be handled via ret-key & ret-field, or a user managed place-channel
(define (go-proc env quoted-form [ret-key #f] [ret-field #f] [place-channel #f])
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))
  (when (and (not (boolean? ret-key)) (not (number? ret-key)))
    (raise-inv-arg "ret-key not a number or #f" ret-key))

  (let ([q-idx (get-min-dp-proc-q-idx env)])
    (semaphore-wait (get-dp-proc-queue-sem env q-idx))
    (enqueue! (get-dp-proc-queue env q-idx) (list quoted-form ret-key ret-field place-channel))
    (semaphore-post (get-dp-proc-queue-sem env q-idx))
    (thread-resume (get-dp-thread env q-idx))
    #t))




;;;----------------------------------------------------------------------------
;;; DATA
;;;----------------------------------------------------------------------------
;; Get the hash of data objects
(define (get-data-hash env)
  (vector-ref (vector-ref (unbox-dp-env env) 4) 0))


;; Get the data objects semaphore
(define (get-data-sem env)
  (vector-ref (vector-ref (unbox-dp-env env) 4) 1))  


;; Get the data object current new key source integer
(define (get-data-key-src env)
  (vector-ref (vector-ref (unbox-dp-env env) 4) 2)) 


;; Set the data object current new key source integer
(define (set-data-key-src env val)
  (vector-set! (vector-ref (unbox-dp-env env) 4) 2 val)) 


;; Get the queue of freed data keys
(define (get-data-free-key-q env)
  (vector-ref (vector-ref (unbox-dp-env env) 4) 3))


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


;; Get a data object reference from the hash
(define (get-data env key)
  (hash-ref (get-data-hash env) key #f))


;; PUBLIC API
;; Get the value of a data object's field
(define (get-data-field env key field)
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))
  (when (and (not (boolean? key)) (not (number? key)))
    (raise-inv-arg "key not a number or #f" key))

  (let ([data (get-data env key)])
    (if data 
      (get-field data field)
      'no-object)))


;; PUBLIC API
;; Redefine data object field)
(define (set-data-field! env key val field)
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))
  (when (and (not (boolean? key)) (not (number? key)))
    (raise-inv-arg "key not a number or #f" key))

  (let ([data (get-data env key)])
    (if data 
      (let ()
        (semaphore-wait (get-data-sem env))
        (set-field! field (get-data env key) val)
        (semaphore-post (get-data-sem env))
        #t)
      #f)))


;; Hash data object at provided key, unless object already present
(define (hash-data! env data-key data)
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))
  (when (and (not (boolean? data-key)) (not (number? data-key)))
    (raise-inv-arg "data-key not a number or #f" data-key))

  (semaphore-wait (get-data-sem env))
  (hash-set! (get-data-hash env) data-key data)
  (semaphore-post (get-data-sem env))
  #t)


;; PUBLIC API
;; Register new data object with the datapool.
(define (register-data! env data)
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))
  (when (not (object? data))
    (raise-inv-arg "data not an object" data))

  (let ([key (gen-dp-data-obj-key env)])
    (if (not (get-data env key)) ;don't hash if data exists 
      (let ()
        (hash-data! env key data)
        key)
      #f)))



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
         [ret-key (car (cdr task))]
         [ret-field (car (cddr task))]
         [ret (co)])
    (if (co 'dead?) ;check if coroutine is dead 
      (let () ;task completed 
        (when (and ret-key ret-field) ;if requested set a data object field with result
          (set-data-field! env ret-key ret-field ret)
          (send-message env (message `(,ret-key ,ret-field changed) ret)))
        #t)
      (let ()
        (go env co ret-key ret-field) ;place task at the back of a queue
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
  (dp-thread env thread-idx))


;; Thread startup coroutine
(define (dp-thread-start env)
  (let ([id (current-thread)])
    (thread-suspend id)
    (define thread-num 0)
    (for ([i (get-num-dp-threads env)])
         (when (equal? (get-dp-thread env i) id) 
           (set! thread-num i)))
    (dp-thread env thread-num)))



;;; process threads
;; Return thread's queue index if not empty, otherwise gets the index of the 
;; fullest queue.
(define (get-task-proc-q-idx env thread-idx)
  (let ([thread-queue (get-dp-proc-queue env thread-idx)])
    (if (equal? (queue-length thread-queue) 0)
      (let ([highest-idx (get-max-dp-proc-q-idx env)])
        (if (equal? (queue-length (get-dp-proc-queue env highest-idx)) 0) 
          #f
          highest-idx))
      thread-idx)))


;; Return a task from a thread queue to execute
(define (get-proc-task env thread-idx)
  (let ([q-idx (get-task-proc-q-idx env thread-idx)])
    (if (equal? q-idx #f)
      #f 
      (let ()
        (semaphore-wait (get-dp-proc-queue-sem env q-idx))
        (let ([ret 
                (if (queue-empty? (get-dp-proc-queue env q-idx))
                  #f
                  (dequeue! (get-dp-proc-queue env q-idx)))])
          (semaphore-post (get-dp-proc-queue-sem env q-idx))
          ret)))))


;; Eternal thread tail recursion of executing external process tasks
(define (dp-proc-thread env thread-idx) 
  (with-handlers 
    ([exn:fail? 
       (lambda (e)
         (fprintf 
           (current-error-port)
           "dp-proc-thread got error: ~a\n"
           e)
         (dp-proc-thread env thread-idx))])
    (let ([task (get-proc-task env thread-idx)])
      (if (equal? (car task) #f)
        (thread-suspend (current-thread))
        (let ([proc-task (list (car task) (car (cdddr task)))])
          (let* ([ret (place-channel-put/get 
                        (get-process-channel env thread-idx) 
                        proc-task)]
                 [ret-key (cdr task)]
                 [ret-field (cddr task)])
            (if (or
                  (equal? ret 'died-before-execution)
                  (equal? ret 'died-during-execution))
              (let ([error-text (place-channel-get 
                                  (get-process-channel env thread-idx))])
                (fprintf 
                  (current-error-port) 
                  "Process died: ~a, Error text: ~a\n"
                  ret 
                  error-text))
              (when (and ret-key ret-field)
                (set-data-field! env ret-key ret-field ret))))))))
  (dp-proc-thread env thread-idx))


;; Thread startup coroutine
(define (dp-process-thread-start env)
  (let ([id (current-thread)])
    (thread-suspend id)
    (define thread-num 0)
    (for ([i (get-num-dp-proc-threads env)])
         (when (equal? (get-dp-thread env i) id) 
           (set! thread-num i)))
    (dp-proc-thread env thread-num)))



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
           payload)))


;; PUBLIC API
;; Create a message object 
(define (message inp-type inp-payload [source-key #f])
  (when (and (not (boolean? source-key)) (not (number? source-key)))
    (raise-inv-arg "source-key not a number or #f" source-key))
  (make-object message% source-key inp-type inp-payload)) 


;; PUBLIC API
;; Return the message's type
(define (message-type msg)
  (when (not (object? msg))
    (raise-inv-arg "msg not an object" msg))
  (get-field type msg))


;; PUBLIC API
;; Return the message's content
(define (message-content msg)
  (when (not (object? msg))
    (raise-inv-arg "msg not an object" msg))
  (get-field payload msg))


;; PUBLIC API
;; Return the message's content
(define (message-source msg)
  (when (not (object? msg))
    (raise-inv-arg "msg not an object" msg))
  (get-field src msg))


;; Get the hash of message callback handlers
(define (get-dp-message-handler-hash env)
  (vector-ref (vector-ref (unbox-dp-env env) 3) 0))


;; Get the message callback handlers semaphore
(define (get-dp-message-handler-hash-sem env)
  (vector-ref (vector-ref (unbox-dp-env env) 3) 1))


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
  (set! ret (and 
              ret 
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
;; suspended invocation). This coroutine should accept 1 message argument and 
;; nothing else.
;;
;; The handler will only execute if an incoming message is sent with a matching
;; src-key to the one specified here. By default this is #f, and will only match 
;; messages with a src-key equal to #f.
;;
;; ret-key and ret-field are passed to the (go) form when the handler is invoked.
(define (register-message-handler 
          env 
          coroutine-procedure 
          msg-type 
          [src-key #f] 
          [ret-key #f] 
          [ret-field #f])
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))
  (when (and (not (boolean? src-key)) (not (number? src-key)))
    (raise-inv-arg "src-key not a number or #f" src-key))
  (when (and (not (boolean? ret-key)) (not (number? ret-key)))
    (raise-inv-arg "ret-key not a number or #f" ret-key))

  (let ([msg-handlers (get-dp-message-handlers env msg-type src-key)])
    (set-dp-message-handlers! 
      env 
      msg-type 
      (append msg-handlers (list (list coroutine-procedure ret-key ret-field))))
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
                  [ret-key (car (cdr i))]
                  [ret-field (car (cddr i))])
              (go env (handler msg) ret-key ret-field)))
          handlers)
        #t)
      #f)))


;; PUBLIC API
;; Send a message to connected handlers in the current datapool
(define (send-message env msg [src-key #f])
  (when (not (vector? (unbox-dp-env env)))
    (raise-inv-arg "env not a vector" (unbox-dp-env env)))
  (when (not (object? msg))
    (raise-inv-arg "msg not an object" msg))
  (when (and (not (boolean? src-key)) (not (number? src-key)))
    (raise-inv-arg "src-key not a number or #f" src-key))

  (go env (send-message-co env msg src-key)))


;; PUBLIC API
;; Data destructor. Removes message handlers, removes the data from the data 
;; hash, and enqueues the now free data key onto a free floating key queue
(define (delete-data! env data-key) 
  (let ([data (get-data env data-key)])
    (if (not (equal? data #f)) ;if data exists
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
            (hash-for-each 
              msg-type-hash 
              (lambda (src-key handlers)
                (let ([new-handlers 
                        (filter 
                          (lambda (h)
                            ;(list coroutine-procedure ret-key ret-field)
                            (not (equal? (cdr h) data-key)))
                          handlers)])
                  (hash-set! (hash-ref 
                               (get-dp-message-handler-hash env)
                               msg-type)
                             src-key
                             new-handlers))))))

        ;Remove the data from the hash
        (hash-remove! (get-data-hash env) data-key)
        (semaphore-post (get-data-sem env))
        (semaphore-post (get-dp-message-handler-hash-sem env))

        (add-free-dp-data-key env data-key)
        #t)
      #f))) 



;;;---------------------------------------------------------------------------- 
;;; TESTING - 3 unit tests
;;;---------------------------------------------------------------------------- 
;; PUBLIC API
(define 
  (run-3-unit-tests [pr #t] [wait #f])
  (reset-test-results)
  (define test-num 1)
  (define (test-text text)
    (let ([ret (string-append text " " (number->string test-num))])
      (set! test-num (+ test-num 1))
      ret))


  ;;**************************************
  ;;TEST defined?
  ;;     test-true? 
  ;;     test-equal?
  ;;     test-pass
  ;;     test-fail
  ;;-------------------------------------- 
  (test-section "test functions")
  (let ()
    (define x 3)
    (test-true? "defined? 1" (if (defined? x) #t #f) pr wait)
    (test-true? "defined? 2" (if (defined? y) #f #t) pr wait)
    (test-true? "test-true? 1" #t pr wait)
    (test-true? "test-true? 2" (not #f) pr wait)
    (test-equal? "test-equal? 3" 1 1 pr wait)
    (test-equal? "test-equal? 4" "test" "test" pr wait)
    (test-equal? "test-equal? 5" 'test 'test pr wait))
  ;;**************************************


  ;;**************************************
  ;;TEST coroutine
  ;;--------------------------------------
  (test-section "coroutines")
  (let ([arg1 3]
        [arg2 4])

    (set! test-num 1)

    (define-coroutine
      (co-test1) 
      (yield 1)
      (yield 2)
      #f)

    (define ct1 (co-test1))

    ;Test the coroutine runs correctly
    (test-true? "new coroutine alive" (ct1 'alive?) pr wait)
    (test-equal? "coroutine yields expected 1" (ct1) 1 pr wait)
    (test-true? "coroutine alive" (ct1 'alive?) pr wait)
    (test-equal? "coroutine yields expected 2" (ct1) 2 pr wait)
    (test-true? "coroutine alive" (ct1 'alive?) pr wait)
    (test-true? "coroutine returns #f" (not (ct1)) pr wait)
    (test-true? "coroutine dead" (ct1 'dead?) pr wait)

    (define-coroutine
      (co-test2 arg1 arg2)
      (yield arg1)
      (yield arg2)
      #f)

    (define ct2 (co-test2 "teststring" 'test))

    ;Test coroutine can be run with arguments
    (test-true? "new coroutine alive" (ct2 'alive?) pr wait)
    (test-equal? "coroutine yields \"teststring\"" (ct2) "teststring" pr wait)
    (test-true? "coroutine alive" (ct2 'alive?) pr wait)
    (test-equal? "coroutine yields 'test" (ct2) 'test pr wait)
    (test-true? "coroutine alive" (ct2 'alive?) pr wait)
    (test-true? "coroutine returns #f" (not (ct2)) pr wait)
    (test-true? "coroutine dead" (ct2 'dead?) pr wait)

    (set! ct2 (co-test2 'test2 "teststring2"))

    ;Test coroutine can be rerun with new arguments
    (test-true? "new coroutine alive" (ct2 'alive?) pr wait)
    (test-equal? "coroutine yields 'test" (ct2) 'test2 pr wait)
    (test-true? "coroutine alive" (ct2 'alive?) pr wait)
    (test-equal? "coroutine yields \"teststring2\"" (ct2) "teststring2" pr wait)
    (test-true? "coroutine alive" (ct2 'alive?) pr wait)
    (test-true? "coroutine" (not (ct2)) pr wait)
    (test-true? "coroutine dead" (ct2 'dead?) pr wait)

    ;Should *not* raise an exception due to the stateful (set!)
    (with-handlers ([exn:fail:user? (lambda (e) (test-fail e))])
                   (define-coroutine 
                     (co-test3 arg1)
                     (yield arg1)
                     (set! arg1 2)
                     (yield arg1))
                   (define ct3 (co-test3 1))
                   (test-equal? "new coroutine yields 1" (ct3) 1 pr wait)
                   (test-equal? "coroutine yields changed argument (2)" (ct3) 2 pr wait)
                   ;Final argument of coroutine is a (yield) so we have to run 
                   ;again to hit the end (even though there are no forms left 
                   ;to evaluate)
                   (test-equal? "coroutine returns 2 (previous return value), because the last form is a (yield) so the coroutine is not technically 'dead yet" (ct3) 2 pr wait)
                   (test-true? "coroutine dead" (ct3 'dead?) pr wait)) 

    ;tail recursion in coroutines should be done with internally defined functions 
    ;for readability. This is because a coroutine invocation (example: (my-couroutine)) 
    ;initially returns the routine in the 'suspended state before evaluating body 
    ;forms. It's confusing to have to do a double invocation (example: ((my-couroutine)))
    ;as a tail form. I'm also not sure it actually *counts* as true tail recursion in that 
    ;sense (that racket will understand what's happening and properly adapt instead
    ;of generating new stack frames)
    (define-coroutine (co-test4 val)
                      (define (recurse-func val)
                        (if (equal? val 0)
                          val
                          (let ()
                            (yield val)
                            (recurse-func (- val 1)))))
                      (recurse-func val))

    (define ct4 (co-test4 3))
    (test-true? "new coroutine alive" (ct4 'alive?) pr wait)
    (test-equal? "recursive function in coroutine yields expected value" (ct4) 3 pr wait)
    (test-equal? "recursive function in coroutine yields expected value" (ct4) 2 pr wait)
    (test-equal? "recursive function in coroutine yields expected value" (ct4) 1 pr wait)
    (test-equal? "recursive function in coroutine returns expected value" (ct4) 0 pr wait)
    (test-true? "coroutine dead" (ct4 'dead?) pr wait)) 
  ;;************************************** 


  ;;**************************************
  ;;TEST channel ch-get ch-put
  ;;--------------------------------------
  (test-section "channels")
  (let ([ch (channel)])
    (test-true? "channel creation" (async-channel? ch) pr wait))

  (let ([ch (channel)])
    (async-channel-put ch "teststring")
    (let ([ret (ch-get ch #f)])
      (test-equal? "ch-get can get a string" ret "teststring" pr wait)))

  (let ([ch (channel)])
    (async-channel-put ch "teststring2")
    (let ([ret (ch-get ch)])
      (test-equal? "ch-get can get a different string" ret "teststring2" pr wait)))

  (let ([ch (channel)])
    (ch-put ch "teststring")
    (let ([ret (ch-get ch #f)])
      (test-equal? "ch-put a string" ret "teststring" pr wait)))

  (let ([ch (channel)])
    (ch-put ch "teststring2")
    (let ([ret (ch-get ch)])
      (test-equal? "ch-put another string" ret "teststring2" pr wait)))
  ;;**************************************


  ;;**************************************
  ;;TEST make-datapool 
  ;;     unbox-dp-env 
  ;;     close-dp
  ;;-------------------------------------- 
  (test-section "datapool data constructor, getter, and destructor functions")
  ;Make a datapool
  (let* ([num-threads 2]
         [num-processes 2]
         [env (make-datapool num-threads num-processes)]) 

    ;;------------------------------------- 
    ;; check num counts are correct
    (test-equal?
      "number of threads"
      (vector-ref (vector-ref (unbox-dp-env env) 0) 0)
      2
      pr
      wait)

    (test-equal?
      "number of processes"
      (vector-ref (vector-ref (unbox-dp-env env) 0) 1)
      2
      pr
      wait)

    ;;-------------------------------------
    ;Verify threads exist
    (for ([i num-threads])
         (test-true? 
           "(and make-datapool get-data) verify threads exist" 
           (thread? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (unbox-dp-env 
                     env) 
                   1) 
                 i) 
               0)) 
           pr 
           wait))

    ;Verify the threads are alive
    (for ([i num-threads])
         (test-true? 
           "(and make-datapool get-data) verify threads are alive" 
           (not 
             (thread-dead? 
               (vector-ref
                 (vector-ref 
                   (vector-ref 
                     (unbox-dp-env 
                       env) 
                     1) 
                   i) 
                 0))) 
           pr 
           wait))

    ;Verify thread task queues exist
    (for ([i num-threads])
         (test-true? 
           "(and make-datapool get-data) verify task queues exist" 
           (queue? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (unbox-dp-env 
                     env) 
                   1) 
                 i) 
               1)) 
           pr 
           wait))

    ;Verify thread task queue semaphores exist
    (for ([i num-threads])
         (test-true? 
           "(and make-datapool get-data) verify task queue semaphores exist" 
           (semaphore? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (unbox-dp-env 
                     env) 
                   1) 
                 i) 
               2)) 
           pr 
           wait))

    ;;------------------------------------- 
    ;Verify place channels exist 
    (for ([i num-processes])
         (let ([place-channels (vector-ref 
                                 (vector-ref 
                                   (vector-ref 
                                     (unbox-dp-env 
                                       env) 
                                     2) 
                                   i) 
                                 0)])
           (let 
             ([o (open-output-string)])
             (fprintf o "Local place channel exists for place process ~a" i)
             (test-true? 
               (get-output-string o)
               (place-channel? (car place-channels))
               pr 
               wait))
           (let 
             ([o (open-output-string)])
             (fprintf o "Remote place channel exists for place process ~a" i)
             (test-true? 
               (get-output-string o)
               (place-channel? (car (cdr place-channels)))
               pr 
               wait))))

    ;Verify place processes exist
    (for ([i num-processes])
         (test-true? 
           "(and make-datapool get-data) verify place processes exist" 
           (place? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (unbox-dp-env 
                     env) 
                   2) 
                 i) 
               1)) 
           pr 
           wait))

    ;Verify process threads exist
    (for ([i num-processes])
         (test-true? 
           "(and make-datapool get-data) verify process threads exist" 
           (thread? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (unbox-dp-env 
                     env) 
                   2) 
                 i) 
               2)) 
           pr 
           wait))

    ;Verify the process threads are alive
    (for ([i num-processes])
         (test-true? 
           "(and make-datapool get-data) verify process threads are alive" 
           (not 
             (thread-dead? 
               (vector-ref
                 (vector-ref 
                   (vector-ref 
                     (unbox-dp-env 
                       env) 
                     2) 
                   i) 
                 2))) 
           pr 
           wait))

    ;Verify process thread task queues exist
    (for ([i num-processes])
         (test-true? 
           "(and make-datapool get-data) verify task queues exist" 
           (queue? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (unbox-dp-env 
                     env) 
                   2) 
                 i) 
               3)) 
           pr 
           wait))

    ;Verify process thread task queue semaphores exist
    (for ([i num-processes])
         (test-true? 
           "(and make-datapool get-data) verify task queue semaphores exist" 
           (semaphore? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (unbox-dp-env 
                     env) 
                   2) 
                 i) 
               4)) 
           pr 
           wait))

    ;;-------------------------------------
    ;Verify hash table of message handlers exists
    (test-true? 
      "(and make-datapool get-data) verify message handler hash table exists" 
      (hash? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env 
              env) 
            3) 
          0)) 
      pr 
      wait)

    ;Verify message hash table semaphore exists
    (test-true? 
      "(and make-datapool get-data) verify message handlers hash table semaphore exists" 
      (semaphore? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env 
              env) 
            3) 
          1)) 
      pr 
      wait)

    ;;-------------------------------------
    ;Verify hash table of data objects exists
    (test-true? 
      "(and make-datapool get-data) verify hash table of data objects exists" 
      (hash? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env 
              env) 
            4) 
          0)) 
      pr 
      wait)

    ;Verify hash table semaphore exists
    (test-true? 
      "(and make-datapool get-data) verify data object hash table semaphore exists" 
      (semaphore? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env 
              env) 
            4) 
          1)) 
      pr 
      wait)

    ;Verify data object key source variable exists
    (test-true? 
      "(and make-datapool get-data) verify data object key source variable exists" 
      (number? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env 
              env) 
            4) 
          2)) 
      pr 
      wait)

    ;Verify data object freed key queue exists
    (test-true? 
      "(and make-datapool get-data) verify data object freed key queue exists" 
      (queue? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env 
              env) 
            4) 
          3)) 
      pr 
      wait)

    (printf "1\n")
    ;Verify we can kill the datapool environment
    (close-dp env)
    (printf "2\n")

    (for ([i num-threads])
         (let ([o (open-output-string)])
           (fprintf o "close-dp thread ~a" i)
           (test-true? 
             (get-output-string o) 
             (thread-dead? 
               (vector-ref 
                 (vector-ref 
                   (vector-ref 
                     (unbox-dp-env 
                       env) 
                     1) 
                   i) 
                 0)) 
             pr 
             wait)))

    (for ([i num-processes])
         (let ([o (open-output-string)])
           (fprintf o "close-dp process thread ~a" i)
           (test-true? 
             (get-output-string o) 
             (thread-dead? 
               (vector-ref 
                 (vector-ref 
                   (vector-ref 
                     (unbox-dp-env 
                       env) 
                     2) 
                   i) 
                 2)) 
             pr 
             wait)))

    (for ([i num-processes])
         (let ([o (open-output-string)])
           (fprintf o "close-dp place process ~a" i)
           (test-equal? 
             (get-output-string o) 
             (place-wait
               (vector-ref 
                 (vector-ref 
                   (vector-ref 
                     (unbox-dp-env 
                       env) 
                     2) 
                   i) 
                 1)) 
             1 ;expect 'complete value' of 1
             pr 
             wait))))
  ;;**************************************


  ;;**************************************
  ;;TEST get-dp-thread 
  ;;     get-dp-queue 
  ;;     get-dp-queue-sem 
  ;;     get-data-hash 
  ;;     get-data-sem 
  ;;     gen-dp-data-obj-key 
  ;;     get-data-key-src 
  ;;     set-data-key-src 
  ;;     get-data-free-key-q
  ;;--------------------------------------
  (test-section "datapool getters & setters")
  (let* ([num-threads 2]
         [num-processes 2]
         [env (make-datapool num-threads num-processes)])

    ;-------------------------------------- 
    ;verify datapool info is correct
    (test-equal?
      "get-num-dp-threads"
      (get-num-dp-threads env)
      2
      pr
      wait)

    (test-equal?
      "get-num-dp-proc-threads"
      (get-num-dp-proc-threads env)
      2
      pr
      wait)

    ;--------------------------------------
    ;verify correct num of threads exist 
    (test-equal? 
      "get-num-dp-threads verify correct num of threads exist" 
      (get-num-dp-threads env) 
      2 
      pr 
      wait)

    ;verify threads exist 
    (test-true? 
      "get-dp-thread verify threads exist 1" 
      (thread? (get-dp-thread env 0)) 
      pr 
      wait)

    (test-true? 
      "get-dp-thread verify threads exist 2" 
      (thread? (get-dp-thread env 1)) 
      pr 
      wait)

    ;verify thread task queues exist
    (test-true? 
      "get-dp-queue verify thread task queues exist 1" 
      (queue? (get-dp-queue env 0)) 
      pr 
      wait)

    (test-true? 
      "get-dp-queue verify thread task queues exist 2" 
      (queue? (get-dp-queue env 1)) 
      pr 
      wait)

    ;verify thread task queues semaphores exist
    (test-true? 
      "get-dp-queue-sem verify thread task queues semaphores exist 1" 
      (semaphore? (get-dp-queue-sem env 0)) 
      pr 
      wait)

    (test-true? 
      "get-dp-queue-sem verify thread task queues semaphores exist 2" 
      (semaphore? (get-dp-queue-sem env 1)) 
      pr 
      wait)

    ;--------------------------------------
    ;verify correct num of process threads exist 
    (test-equal? 
      "get-num-dp-proc-threads verify correct num of threads exist" 
      (get-num-dp-proc-threads env) 
      2 
      pr 
      wait)

    ;verify threads exist 
    (test-true? 
      "get-dp-proc-thread verify threads exist 1" 
      (thread? (get-dp-proc-thread env 0)) 
      pr 
      wait)

    (test-true? 
      "get-dp-proc-thread verify threads exist 2" 
      (thread? (get-dp-proc-thread env 1)) 
      pr 
      wait)

    ;verify thread task queues exist
    (test-true? 
      "get-dp-proc-queue verify thread task queues exist 1" 
      (queue? (get-dp-proc-queue env 0)) 
      pr 
      wait)

    (test-true? 
      "get-dp-proc-queue verify thread task queues exist 2" 
      (queue? (get-dp-proc-queue env 1)) 
      pr 
      wait)

    ;verify thread task queues semaphores exist
    (test-true? 
      "get-dp-proc-queue-sem verify thread task queues semaphores exist 1" 
      (semaphore? (get-dp-proc-queue-sem env 0)) 
      pr 
      wait)

    (test-true? 
      "get-dp-proc-queue-sem verify thread task queues semaphores exist 2" 
      (semaphore? (get-dp-proc-queue-sem env 1)) 
      pr 
      wait)

    ;--------------------------------------
    ;verify data object hash exists
    (test-true? 
      "get-data-hash verify data object hash exists" 
      (hash? (get-data-hash env)) 
      pr 
      wait) 

    (test-true? 
      "get-data-sem verify data object hash semaphore exists" 
      (semaphore? (get-data-sem env)) 
      pr 
      wait)

    ;verify key generation
    (test-equal? 
      "gen-dp-data-obj-key verify key generation 1" 
      (gen-dp-data-obj-key env) 
      0 
      pr 
      wait)

    (test-equal? 
      "gen-dp-data-obj-key verify key generation 2" 
      (gen-dp-data-obj-key env) 
      1 
      pr 
      wait)

    (test-equal? 
      "gen-dp-data-obj-key verify key generation 3" 
      (gen-dp-data-obj-key env) 
      2 
      pr 
      wait)

    (test-equal? 
      "gen-dp-data-obj-key verify key generation 4" 
      (gen-dp-data-obj-key env) 
      3 
      pr 
      wait)

    ;verify can get data object key source number  
    (test-equal? "get-data-key-src" (get-data-key-src env) 4 pr wait)
    (test-equal? "get-data-key-src" (get-data-key-src env) 4 pr wait)

    ;verify can set data object key source number (?? should never do this?)
    (set-data-key-src env 2)
    (test-equal? "get-data-key-src" (get-data-key-src env) 2 pr wait) 

    ;verify get-data-free-key-q
    (test-true? "get-data-free-key-q" (queue? (get-data-free-key-q env)) pr wait)

    (close-dp env))
  ;;**************************************

  ;;**************************************
  ;;TEST get-min-dp-q-idx 
  ;;     get-max-dp-q-idx 
  ;;     go
  ;;--------------------------------------
  (test-section "task queue getters & setters")
  (let* ([num-threads 2]
         [num-processes 2]
         [env (make-datapool num-threads num-processes)])

    ;;Arbitrary coroutine to execute
    (define (test-task) #t)

    ;;Test defaults
    ;q0-size: 0, q1-size: 0
    (test-equal? "get-min-dp-q-idx 1" (get-min-dp-q-idx env) 0 pr wait)
    (test-equal? "get-max-dp-q-idx 1" (get-max-dp-q-idx env) 0 pr wait)
    (test-equal? "q-len idx 0" (queue-length (get-dp-queue env 0)) 0 pr wait)
    (test-equal? "q-len idx 1" (queue-length (get-dp-queue env 1)) 0 pr wait)

    (enqueue! (get-dp-queue env 0) test-task)

    ;q0-size: 1, q1-size: 0
    (test-equal? "get-min-dp-q-idx 2" (get-min-dp-q-idx env) 1 pr wait)
    (test-equal? "get-max-dp-q-idx 2" (get-max-dp-q-idx env) 0 pr wait)
    (test-equal? "q-len idx 0" (queue-length (get-dp-queue env 0)) 1 pr wait)
    (test-equal? "q-len idx 1" (queue-length (get-dp-queue env 1)) 0 pr wait)

    (enqueue! (get-dp-queue env 1) test-task)

    ;q0-size: 1, q1-size: 1
    (test-equal? "get-min-dp-q-idx 3" (get-min-dp-q-idx env) 0 pr wait)
    (test-equal? "get-max-dp-q-idx 3" (get-max-dp-q-idx env) 0 pr wait)
    (test-equal? "q-len idx 0" (queue-length (get-dp-queue env 0)) 1 pr wait)
    (test-equal? "q-len idx 1" (queue-length (get-dp-queue env 1)) 1 pr wait)

    (enqueue! (get-dp-queue env 1) test-task)

    ;q0-size: 1, q1-size: 2
    (test-equal? "get-min-dp-q-idx 4" (get-min-dp-q-idx env) 0 pr wait)
    (test-equal? "get-max-dp-q-idx 4" (get-max-dp-q-idx env) 1 pr wait)
    (test-equal? "q-len idx 0" (queue-length (get-dp-queue env 0)) 1 pr wait)
    (test-equal? "q-len idx 1" (queue-length (get-dp-queue env 1)) 2 pr wait)

    (close-dp env))
  ;;**************************************


  ;;**************************************
  ;;TEST hash-data!
  ;;     get-data  
  ;;     register-data!
  ;;--------------------------------------
  (test-section "manage data objects")
  (let* ([num-threads 2]
         [num-processes 2]
         [env (make-datapool num-threads num-processes)]) 

    (define test-key 1337)

    (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) 0 pr wait)
    (test-true? "hash-data! number" (hash-data! env test-key 3) pr wait)
    (test-true? "hash-data! string" (hash-data! env test-key "3") pr wait)
    (test-true? "hash-data! list" (hash-data! env test-key (list 3)) pr wait)
    (test-true? "hash-data! quote" (hash-data! env test-key '(3)) pr wait)

    (define test-class%
      (class object% (super-new)
             (field [3-field 3])
             (define/public (get-3) 3-field)))

    (define test-object (make-object test-class%))

    (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) 1 pr wait)
    (test-true? "hash-data! 1" (hash-data! env test-key test-object) pr wait)
    (test-true? "hash-data! 2" (hash-data! env test-key test-object) pr wait)

    (test-equal? "get-data 1" (send (get-data env test-key) get-3) 3 pr wait)
    (test-true? "get-data 2" (not (get-data env 1)) pr wait) 

    (set-field! 3-field (get-data env test-key) 2)
    (test-equal? "set-field succeeded?" (get-field 3-field (get-data env test-key)) 2 pr wait) 

    #|
    (with-handlers 
      ([exn:fail? 
         (lambda (e)
           (test-pass 
             "register-data! number correctly fails" 
             3
             pr)
           (abort-current-continuation (default-continuation-prompt-tag) void))])
      (register-data! env 3)
      (test-fail 
        "register-data! number incorrectly succeeds" 
        3
        pr 
        wait))

    (with-handlers 
      ([exn:fail? 
         (lambda (e)
           (test-pass 
             "register-data! string correctly fails" 
             "\"3\""
             pr)
           (abort-current-continuation (default-continuation-prompt-tag) void))])
      (register-data! env "3")
      (test-fail 
        "register-data! string incorrectly succeeds" 
        "\"3\""
        pr 
        wait))

    (with-handlers 
      ([exn:fail? 
         (lambda (e)
           (test-pass 
             "register-data! list correctly fails" 
             "(list 3)"
             pr)
           (abort-current-continuation (default-continuation-prompt-tag) void))])
      (register-data! env (list 3))
      (test-fail 
        "register-data! list incorrectly succeeds" 
        "(list 3)"
        pr 
        wait))

    (with-handlers 
      ([exn:fail? 
         (lambda (e)
           (test-pass 
             "register-data! quote correctly fails" 
             '(3)
             pr)
           (abort-current-continuation (default-continuation-prompt-tag) void))])
      (register-data! env '(3))
      (test-fail 
        "register-data! quote incorrectly succeeds" 
        '(3)
        pr 
        wait))
    |#

    (let ([hash-size (hash-count (get-data-hash env))])
      (let ([key (register-data! env test-object)])
        (test-equal? "register-data! object succeeds" key 0 pr wait)
        (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) (+ hash-size 1) pr wait)))
    (let ([hash-size (hash-count (get-data-hash env))])
      (let ([key (register-data! env test-object)])
        (test-equal? "register-data! object succeeds" key 1 pr wait)
        (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) (+ hash-size 1) pr wait)))
    (close-dp env))
  ;;**************************************


  ;;**************************************
  ;;TEST message%
  ;;     message 
  ;;     message-type 
  ;;     message-source
  ;;     message-content 
  ;;     get-dp-message-handler-hash
  ;;     get-dp-message-handler-hash-sem
  ;;     set-dp-message-handlers!
  ;;     register-message-handler 
  ;;     send-message-co
  ;;     send-message 
  ;;     delete-data!
  ;;--------------------------------------
  (test-section "manage message handlers")
  (let* ([num-threads 2]
         [num-processes 2]
         [env (make-datapool num-threads num-processes)])

    ;;TODO:
    ;;     message%
    ;;     message 
    ;;     message-type 
    ;;     message-source
    ;;     message-content 

    (test-true? "get-dp-message-handler-hash" (hash? (get-dp-message-handler-hash env)) pr wait)
    (test-true? "get-dp-message-handler-hash-sem" (semaphore? (get-dp-message-handler-hash-sem env)) pr wait)

    (define test-type 'test-type)
    (define test-source 12)
    (define callback-form (lambda () 1))
    (test-equal? "callback check" (callback-form) 1 pr wait)

    (let ([hash-ret (set-dp-message-handlers! env test-type test-source (list (list callback-form test-source #f)))])
      (test-true? "set-dp-message-handlers!" hash-ret pr wait))


    (let ([hash-ret (hash-ref (get-dp-message-handler-hash env) test-type #f)])
      (test-true? "returned value is a hash" (hash? hash-ret) pr wait)
      (test-equal? "(and set-dp-message-handlers! get-dp-message-handler-hash) 1" 
                   ((car (car (hash-ref hash-ret test-source #f))))
                   (callback-form) 
                   pr 
                   wait))

    (define callback-form-2 (lambda () 2))
    (test-equal? "callback check 2" (callback-form-2) 2 pr wait)

    (set-dp-message-handlers! 
      env 
      test-type 
      test-source
      (append 
        (hash-ref (hash-ref (get-dp-message-handler-hash env) test-type) test-source)
        (list (list callback-form-2 test-source #f))))

    (test-equal? "(and set-dp-message-handlers! get-dp-message-handler-hash) 2" 
                 (hash-ref (hash-ref (get-dp-message-handler-hash env) test-type) test-source)
                 (list (list callback-form test-source #f) (list callback-form-2 test-source #f))
                 pr 
                 wait)
    (let ([pre-handlers (list 
                          (list callback-form test-source #f) 
                          (list callback-form-2 test-source #f))]
          [post-handlers (hash-ref (hash-ref (get-dp-message-handler-hash env) test-type) test-source)])
      (for ([i (length pre-handlers)])
           (let ([str 
                   (let ([o (open-output-string)])
                     (fprintf o "compare callback results ~a" i)
                     (get-output-string o))])
             (test-equal? str
                          ((car (list-ref pre-handlers i)))
                          ((car (list-ref post-handlers i)))
                          pr
                          wait))))

    (define test-class%
      (class object% (super-new)
             (field [3-field 3])
             (define/public (get-3) 3-field)))

    (define test-object (make-object test-class%))

    ;;TODO:
    ;;     register-message-handler 
    ;;     delete-data! (make sure message handler is removed)

    (let ([hash-size (hash-count (get-data-hash env))])
      (let ([key (register-data! env test-object)])
        (test-equal? "register-data! object succeeds" key 0 pr wait)
        (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) (+ hash-size 1) pr wait)
        (test-true? "delete-data!" (delete-data! env key) pr wait)
        (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) hash-size pr wait)
        (test-true? "get-data fails" (not (get-data env key)) pr wait)))
    (close-dp env))
  ;;**************************************

  ;;**************************************
  ;;TEST get-task-q-idx
  ;;     get-task
  ;;     dp-thread-exec-task
  ;;     dp-thread
  ;;     dp-thread-start
  ;;     go
  ;;-------------------------------------- 
  #|
  (test-section "datapool thread internal functions")
  (let* ([num-threads 2]
         [num-processes 2]
         [env (make-datapool num-threads num-processes)])
    (define-coroutine (test-task-co)
                      3)
    (define test-task (test-task-co))

    (enqueue! (get-dp-queue env 0) (list test-task #f #f))

    (test-equal? "get-task-q-idx" (get-task-q-idx env 0) 0 pr wait)

    (dequeue! (get-dp-queue env 0))
    (enqueue! (get-dp-queue env 1) (list test-task #f #f))

    (test-equal? "get-task-q-idx 0" (get-task-q-idx env 0) 1 pr wait)
    (test-equal? "get-task-q-idx 1" (get-task-q-idx env 1) 1 pr wait)

    (enqueue! (get-dp-queue env 1) (list test-task #f #f))

    (test-equal? "get-task-q-idx 0" (get-task-q-idx env 0) 1 pr wait)
    (test-equal? "get-task-q-idx 1" (get-task-q-idx env 1) 1 pr wait)

    (enqueue! (get-dp-queue env 0) (list test-task #f #f))

    (test-equal? "get-task-q-idx 0" (get-task-q-idx env 0) 0 pr wait)
    (test-equal? "get-task-q-idx 1" (get-task-q-idx env 1) 1 pr wait)

    (let ([len-0 (queue-length (get-dp-queue env 0))]
          [task (get-task env 0)])
      (test-true? "get-task" (> len-0 (queue-length (get-dp-queue env 0))) pr wait))

    (define inp-vals (list 'test 'test2 #f "teststring"))
    (define ch (channel))
    (define-coroutine (test-task-co2 ch vals)
                      (for ([val vals])
                           (let ()
                             (printf "Putting ~a in channel\n" val)
                             (ch-put ch val))))
    (go env (test-task-co2 ch inp-vals))

    (sleep 0.1)
    (for ([i num-threads])
         (printf "\n--- tests for thread ~a ---\n" i)
         (test-true? "Check if dp thread is not dead" (not (thread-dead? (get-dp-thread env 1))) pr wait)
         (test-true? "Check if dp thread is not running" (not (thread-running? (get-dp-thread env 1))) pr wait)
         (test-equal? "Verify task queue is empty" (queue-length (get-dp-queue env 0)) 0 pr wait))


    (printf "\n")
    (for ([i (length inp-vals)])
         (let ([val (list-ref inp-vals i)]
               [ret (ch-get ch inp-vals)])
           (test-equal? "Did expected val get placed in channel" ret val pr wait)))


    (define inp-vals2 (list 'test3 'test4 #t "teststring2"))
    (go env (test-task-co2 ch inp-vals2))
    (sleep 0.1)

    (for ([i num-threads])
         (printf "\n--- tests for thread ~a ---\n" i)
         (test-true? "Check if dp thread is dead" (not (thread-dead? (get-dp-thread env 1))) pr wait)
         (test-true? "Check if dp thread is actively running" (not (thread-running? (get-dp-thread env 1))) pr wait)
         (test-equal? "Verify task queue is empty" (queue-length (get-dp-queue env 0)) 0 pr wait))

    (printf "\n")
    (for ([i (length inp-vals2)])
         (let ([val (list-ref inp-vals2 i)]
               [ret (ch-get ch #f)])
           (test-equal? "Did expected val get placed in channel" ret val pr wait)))
    (close-dp env))
  |#
  ;;**************************************

  ;wait until total task queue lengths == 0
  (define (wait-len env)
    (define idxs (list))
    (for ([i (get-num-dp-threads env)])
         (set! idxs (append idxs (list i))))
    (define check-in-time (current-inexact-milliseconds))
    (define (inner-loop env idxs)
      (let ([lens (map (lambda (idx) (queue-length (get-dp-queue env idx))) idxs)]
            [done #t])
        (for-each 
          (lambda (len) 
            (when (> len 0) (set! done #f)))
          lens)
        (when (not done)
          (let ()
            (when (> (- (current-inexact-milliseconds) check-in-time) 5000)
              (set! check-in-time (current-inexact-milliseconds))
              (for ([i idxs])
                   (printf "len task q ~a: ~a; " i (queue-length (get-dp-queue env i))))
              ;(printf " ~a; " (queue-length (get-dp-queue env i))))
              (printf "\nWaiting for all tasks to complete...\n"))
            (sleep 0.01)
            (inner-loop env idxs)))))
    (inner-loop env idxs))

  (define (iterations-per-second milli iter)
    (/ iter (/ milli 1000)))

  ;;**************************************
  ;;TEST go ;stress test
  ;;--------------------------------------
  (test-section "go stress test: basic (go) invocations")
  (let* ([num-threads 8]
         [num-processes 0]
         [env (make-datapool num-threads num-processes)])

    (define-coroutine
      (test-ro inp-x)
      (define (inner x)
        (if (equal? x 0)
          #t
          (let ([x (- x 1)])
            (inner x))))
      (inner inp-x))


    (let ([start-time (current-inexact-milliseconds)]
          [v 1000]
          [go-count 0]
          [iterations 0])
      (for ([i v])
           (for ([x i]) 
                (set! go-count (+ go-count 1))
                (go env (test-ro x))
                (set! iterations (+ iterations x)))

           (when (equal? (remainder i 100) 0)
             (printf "\tfinished i: ~a\n" i)))

      (wait-len env)
      (for ([i num-threads])
           (let ([o (open-output-string)])
             (fprintf o "length q[~a]" i)
             (test-equal? (get-output-string o) (queue-length (get-dp-queue env i)) 0 pr wait)))
      (let ([time (- (current-inexact-milliseconds) start-time)])
        (printf "Benchmark time (milli) for ~a loop iterations. That means u through ~a iterations with i through u (go) calls (each evaluation looping i times, where i is the current (go) iteration), in ~a threads, with no yields or returns: ~a\n" iterations v num-threads time)
        (printf "(go) invocations: ~a\n" go-count)
        (printf "loop iterations per second: ~a\n" (iterations-per-second time iterations))))

    (close-dp env)) 
  ;;--------------------------------------

  ;;**************************************
  ;;TEST go ;stress test 2
  ;;--------------------------------------

  (test-section "go stress test 2: timing comparisions for addition")
  (let* ([num-threads 8]
         [num-processes 0]
         [env (make-datapool num-threads num-processes)])

    (sleep 0.1)
    (define-coroutine 
      (go-return x)
      x)

    (let ([start-time (current-inexact-milliseconds)]
          [x 8000])
      (for ([i x])
        (go env (go-return i)))
      (wait-len env)
      (let ([time (- (current-inexact-milliseconds) start-time)])
        (printf "Benchmark time (milli) for ~a immediately returning (go) operations: ~a\n" x time)
        (printf "(go) operations per second: ~a\n\n" (iterations-per-second time x))))
    ;------------------------------------------------------------------------
    (sleep 0.5)


    (let ([x 1000]
          [inner-x 1000000]) ;1,000,000 
      (define-coroutine 
        (eval-x-times env inp-x [do-yield #f])
        (define (in-loop x)
          (if (equal? x 0)
            #t
            (let ([x (- x 1)])
              (when do-yield (yield x))
              (in-loop x))))
        (in-loop inp-x))

      ;------------------------------------------------------------------------
      (sleep 0.5)

      (let ([start-time (current-inexact-milliseconds)]
            [iterations 0])
        (for ([u num-threads])
             (for ([i x])
                  (set! iterations (+ iterations x))
                  (go env (eval-x-times env x #f))))
        (wait-len env)
        (let ([time (- (current-inexact-milliseconds) start-time)])
          (printf "Benchmark time (milli) for ~a (go) calls with ~a evaluations on ~a threads with (when yield) checks: ~a\n"  (* x num-threads) x num-threads time)
          (printf "loop iterations per second: ~a\n\n" (iterations-per-second time iterations))))
      ;------------------------------------------------------------------------ 

      (sleep 0.5)

      (let ([start-time (current-inexact-milliseconds)]
            [iterations 0])
        (for ([i num-threads])
             (set! iterations (+ iterations inner-x))
             (go env (eval-x-times env inner-x)))
        (wait-len env)
        (let ([time (- (current-inexact-milliseconds) start-time)])
          (printf "Benchmark time (milli) for ~a (go) calls with ~a evaluations on ~a threads in coroutine without yield calls: ~a\n"  num-threads inner-x num-threads time)
          (printf "loop iterations per second: ~a\n\n" (iterations-per-second time iterations))))

      ;------------------------------------------------------------------------ 

      (sleep 0.5)

      ;parallel processing with futures
      (define-coroutine
        (eval-x-times-parallel env inp-x)
        (define (in-loop x)
          (if (equal? x 0)
            #t
            (in-loop (- x 1))))
        (future (thunk (in-loop inp-x))))


      (let ([start-time (current-inexact-milliseconds)]
            [iterations 0])
        (for ([i num-threads])
             (set! iterations (+ iterations inner-x))
             (go env (eval-x-times-parallel env inner-x)))
        (wait-len env)
        (let ([time (- (current-inexact-milliseconds) start-time)])
          (printf "Benchmark time (milli) for ~a (go) calls with ~a evaluations on ~a threads and ~a parallel processed futures: ~a\n"  num-threads inner-x num-threads 8 time)
          (printf "loop iterations per second: ~a\n\n" (iterations-per-second time iterations)))))
    ;------------------------------------------------------------------------ 

    (close-dp env))
  ;;**************************************
  

  ;;**************************************
  ;;TEST go stress test 3 ;results of calculation
  ;;--------------------------------------
  (test-section "go stress test 3: collating results")
  (let* ([num-threads 8]
         [num-processes 0]
         [env (make-datapool num-threads num-processes)]
         [x 1000000]
         [ch (channel)])
    (close-dp env))

  ;;**************************************
  ;;TEST go ;datapool interactions 
  ;;     send-message-co
  ;;     send-message
  ;;     set-data-field!
  ;;--------------------------------------

  ;;**************************************
  ;;TEST go-proc
  ;;--------------------------------------

  ;;**************************************
  ;;TEST go-proc; stress test
  ;;--------------------------------------

  ;;**************************************
  ;;TEST go-proc ;datapool interactions set-data-field! and send-message
  ;;--------------------------------------

  ;;;---------------------------------------------------------------------------- 
  ;;; Feature Tests
  ;;;---------------------------------------------------------------------------- 
  ;TODO figure out how to get argv & argc 
  ;(define dp1 (make-datapool 4 '(main argv argc)))
  ;(let ([ch (get-datapool-input-channel dp1)]) 
  ;(ch-get ch))


  ;;;---------------------------------------------------------------------------- 
  ;;; Closing Analysis
  ;;;---------------------------------------------------------------------------- 
  (print-test-report))
