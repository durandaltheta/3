#lang racket
(require racket/base
         racket/class
         racket/async-channel
         data/queue)
;delay-pure) 

(provide datapool ;start a datapool with given env data and form to execute
         close-dp ;kill all threads in the datapool
         channel ;create an asynchronous channel
         ch-get ;get form from a channel
         ch-put ;send form into a channel
         get-datapool-input-channel ;get the channel to send messages into the dp
         send-to-datapool-output-channel ;send data to the datapool's parent scope
         data ;create data object and register it in the datapool 
         get-dp-data-object ;get an object via its key
         message% ;create a message object with a given type and arguments
         go ;place a coroutine in a queue to be executed by a thread
         definep ;define a pure & stateless coroutinetion
         coroutine ;return a coroutine thunk for use in (go)
         yield
         ;danger-coroutine! ;define a coroutine with no state checks
         test-true?
         test-equal?
         test-fail
         test-pass
         run-unit-tests)

;;;----------------------------------------------------------------------------
;;; Test functions 
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
  (for ([i 80])
       (printf "~a" char))
  (printf "\n"))

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

;Print final test results
(define (print-test-section-report)
  (printf "\nNumber of test passes: ~a\n" *num-passes*)
  (printf "Number of test failures: ~a\n\n" *num-fails*)
  (printf "Failed tests:\n")
  (for ([i *failed-tests*])
       (printf "\t~a" i))
  (reset-test-results))

;Designate & print current test section with description
(define (TEST-SECTION name [print #t])
  (when print
    (let ()
      (set! *cur-test-section* name)
      (if *tests-started*
        (let ()
          (print-test-section-report)
          (printf "\n\n"))
        (set! *tests-started* #t))
      (print-test-divider "#")
      (printf "### TEST-SECTION ~a\n" name)
      (print-test-divider "#"))))

;; Return #t if the quoted form returns #t, else #f 
(define (test-true? description form [print-result #t] [wait #f])
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

;; Return #t if quoted forms return an equal value, else #f
(define (test-equal? description form-a form-b [print-result #t] [wait #f])
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


;; Custom test fail
(define (test-fail description form [print-result #t] [wait #f])
  (let ([o (open-output-string)])
    (fprintf o "FAIL TEST ~a ~a: ~a\n" (get-test-num) description form)
    (when print-result
      (printf (get-output-string o))
      (when wait
        (read-line (current-input-port) 'any)))
    (let ([ret (cons #f (get-output-string o))])
      (collate-test ret)
      ret)))


;; Custom test pass
(define (test-pass description form [print-result #t])
  (let ([o (open-output-string)])
    (fprintf o "pass TEST ~a ~a: ~a\n" (get-test-num) description form)
    (when print-result
      (printf (get-output-string o)))
    (let ([ret (cons #t (get-output-string o))])
      (collate-test ret)
      ret)))



;;;----------------------------------------------------------------------------
;;; Pure/Stateless Coroutines
;;;----------------------------------------------------------------------------  
(define (yield value) value)

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
                     ((eq? (car args) 'coroutine?) #t)
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


;;Return a suspended coroutine created from given args and body forms
(define-syntax (coroutine stx)
  (syntax-case stx ()
               ((_ (args ...) . body )
                #`((lambda (args ...)
                     (make-generator
                       (lambda (#,(datum->syntax stx 'yield))
                         . body)))))))


;; Define a namespace so eval can run correctly
(define-namespace-anchor eval-anchor)
(define eval-namespace (namespace-anchor->namespace eval-anchor))


;; Define a pure & stateless coroutinetion
(define-syntax-rule (definep (name . params) body ...)
                    (define-pure/stateless (name . params) body ...))


;;TODO: Not sure what to do about not being able to use 
;;delay/pure as expected in untyped racket
;; Define a coroutine of a pure stateless coroutine. These should always 
;; be safe when executed asynchronously with a (go) call. The current 
;; 'coroutine' portion of the definition *is* stateful, only the provided form 
;; is guaranteed to be stateless 
#|
(define-syntax-rule (define-coroutine (name . params) body ...)
                    (eval (let ()
                            ;Special check to see if given coroutinetion symbols 
                            ;produce a pure & stateless coroutinetion. Otherwise
                            ;should raie an error!
                            (let ([def (definep (name . params)
                                                body
                                                ...)])
                              (void))
                            '(define-coroutine (name . params)
                                               (begin
                                                 (yield 'alive) 
                                                 body
                                                 ...))) 
                          eval-namespace))


;; DO *NOT* USE UNLESS REQUIRED FOR PERFORMANCE REASONS
;; Define a coroutine with no purity/state checks. (define-coroutine) can be slow because 
;; it fully expands the form checking for purity/statelessness. This rule skips
;; that check, which makes it *dangerous* when used in the multithreaded (go)
;; environment. Therefore, even if you use this coroutinetion you should still 
;; validate its purity, manually if necessary.
(define-syntax-rule (danger-coroutine! (name . params) body ...)
                    (define-coroutine (name . params)
                                      (begin
                                        (yield 'alive) 
                                        body
                                        ...)))
|#


;;;----------------------------------------------------------------------------
;;;basic channel functions
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



;;;--------------------------------------------------------------------------
;;; Datapool Data functions
;;;--------------------------------------------------------------------------
;;create datapool data 
(define (make-dp-data num-threads eval-limit) 
  (let* ([key-src 0]
         [ret
           (box 
             (vector 
               ;thread id's, queues, and semaphores
               (make-vector ;thread and task queue vector
                 num-threads 
                 ;(vector 
                 ;  (thread (thunk (dp-thread-start ret))) ;threads
                 ;  (make-queue) ;thread task queues
                 ;  (make-semaphore 1))) ;thread task queue semaphores 
                 )
               (vector ;message handler vector
                 (make-hash) ;hash table of data objects
                 (make-hash) ;hash table of lists of message handlers
                 (make-semaphore 1) ;data hash semaphore
                 (make-semaphore 1)) ;message handlers semaphore
               (vector ;datapool channel vector
                 (channel #f) ;parent->tdatapool channel
                 (channel #f) ;datapool->parent channel
                 (make-semaphore 1)) ;datapool->parent channel semaphore 
               (vector ;data object vector
                 key-src ;data-obj-key-src
                 (make-queue)) ;data-obj-free-key-q 
               (vector ;evaluation rules vector
                 eval-limit)))])
    (for ([i num-threads])
         (let ()
           (vector-set! 
             (vector-ref (unbox ret) 0) 
             i
             (vector 
               (thread (thunk (dp-thread-start ret))) ;threads
               (make-queue) ;thread task queues
               (make-semaphore 1))))) ;thread task queue semaphores 
    ret)) ;return a 'by reference' symbol


;; Return the datapool's data
(define (get-dp-data env)
  (unbox env))


;; Return number of threads in the datapool
(define (get-num-dp-threads env) 
  (vector-length (vector-ref (get-dp-data env) 0)))

;;kill all threads in a datapool
(define (close-dp env)
  (for ([i (vector-length (vector-ref (get-dp-data env) 0))])
       (kill-thread (vector-ref (vector-ref (vector-ref (get-dp-data env) 0) i) 0))))


;; Get the communication parentch-putdatapool channel
(define (get-datapool-input-channel env) 
  (vector-ref (vector-ref (get-dp-data env) 2) 0))



;;;--------------------------------------------------------------------------
;;; Start private datapool thread coroutinetion defines
;;;-------------------------------------------------------------------------- 
;; Get a thread pid at provided index
(define (get-dp-thread env idx)
  (vector-ref (vector-ref (vector-ref (get-dp-data env) 0) idx) 0))


;; Get a thread task queue at provided index
(define (get-dp-queue env idx)
  (vector-ref (vector-ref (vector-ref (get-dp-data env) 0) idx) 1))


;; Get a thread task queue semaphore at provided index
(define (get-dp-queue-sem env idx)
  (vector-ref (vector-ref (vector-ref (get-dp-data env) 0) idx) 2))


;; Get the hash of data objects
(define (get-dp-data-objects env)
  (vector-ref (vector-ref (get-dp-data env) 1) 0))


;; Get the data objects semaphore
(define (get-dp-data-objects-sem env)
  (vector-ref (vector-ref (get-dp-data env) 1) 2))  


;; Get the data object current new key source integer
(define (get-data-obj-key-src env)
  (vector-ref (vector-ref (get-dp-data env) 3) 0)) 


;; Set the data object current new key source integer
(define (set-data-obj-key-src env val)
  (vector-set! (vector-ref (get-dp-data env) 3) 0 val)) 


;; Get the queue of freed data keys
(define (get-data-obj-free-key-q env)
  (vector-ref (vector-ref (get-dp-data env) 3) 1))


(define (get-dp-thread-continuous-eval-limit env)
  (vector-ref (vector-ref (get-dp-data env) 4) 0))


;; Add a recycled data object key to the container queue
(define (add-free-dp-data-key env key) 
  (enqueue! (get-data-obj-free-key-q env) key))


;; Generate a new data object hash key
(define (gen-dp-data-obj-key env) 
  (if (> (queue-length (get-data-obj-free-key-q env)) 0) 
    (dequeue! (get-data-obj-free-key-q env))
    (let ([ret (get-data-obj-key-src env)])
      (set-data-obj-key-src env (+ (get-data-obj-key-src env) 1)) 
      ret)))


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


;;Enqueues coroutine instance to the emptiest thread queue and resumes the 
;;thread. The task is typically internally a coroutine
(define (go env suspended-coroutine)
  (let ([q-idx (get-min-dp-q-idx env)])
    (semaphore-wait (get-dp-queue-sem env q-idx))
    (enqueue! (get-dp-queue env q-idx) suspended-coroutine)
    (semaphore-post (get-dp-queue-sem env q-idx))
    (thread-resume (get-dp-thread env q-idx))
    #t))


;; Get a data object from the hash
(define (get-dp-data-object env key)
  (hash-ref (get-dp-data-objects env) key #f))


;; Hash new data object
(define (hash-dp-data-object env obj-key obj)
  (if (equal? (get-dp-data-object env obj-key) #f)
    (let ()
      (semaphore-wait (get-dp-data-objects-sem env))
      (hash-set! (get-dp-data-objects env) obj-key obj)
      (semaphore-post (get-dp-data-objects-sem env))
      (define-coroutine 
        (run-handler)
        (send (get-dp-data-object env obj-key) run))
      (define rh (run-handler))
      (go env rh)
      #t)
    #f)) ;we've run out of possible hash table values and looped?


;; Delete and remove a data object from the hash
(define (delete-dp-data-object env key)
  (semaphore-wait (get-dp-data-objects-sem env))
  (hash-set! (get-dp-data-objects env) key #f)
  (semaphore-post (get-dp-data-objects-sem env))
  #t)


;; Get the hash of message callback handlers
(define (get-dp-message-handlers env)
  (vector-ref (vector-ref (get-dp-data env) 1) 1))


;; Get the message callback handlers semaphore
(define (get-dp-message-handlers-sem env)
  (vector-ref (vector-ref (get-dp-data env) 1) 3))


;; Set the global message handlers to something new
(define (set-dp-message-handlers env msg-type handlers) 
  (semaphore-wait (get-dp-message-handlers-sem env))
  (hash-set! (get-dp-message-handlers env) msg-type handlers)
  (semaphore-post (get-dp-message-handlers-sem env)))


;; Get the parent->dp channel
(define (get-datapool-output-channel env) 
  (vector-ref (vector-ref (get-dp-data env) 2) 1))


;; Get the semaphore for the parent->dp channel
(define (get-dp-parent-ch-sem env)
  (vector-ref (vector-ref (get-dp-data env) 2) 2))


;; Send info from the datapool to the parent
(define (send-to-datapool-output-channel env form)
  (semaphore-wait (get-dp-parent-ch-sem env))
  (ch-put (get-datapool-output-channel env) form)
  (semaphore-post (get-dp-parent-ch-sem env)))


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
        (if (queue-empty? (get-dp-queue env q-idx))
          (let ()
            (semaphore-post (get-dp-queue-sem env q-idx))
            #f)
          (let ()
            (if (queue-empty? (get-dp-queue env q-idx))
              #f
              (let ()
                (define task (dequeue! (get-dp-queue env q-idx)))
                (semaphore-post (get-dp-queue-sem env q-idx))
                task))))))))


;; Evaluate given task, limits cpu starvation by limiting continuous 
;; evaluations in coroutines. If provided task is *not* a coroutine and/or 
;; that coroutine does not (yield) intelligently this may have no effect.
;; Returns: #t if task completed, #f if task not yet completed
(define (dp-thread-exec-task env thread-idx task evals-left)
  ;(printf "exec-task thread-idx: ~a\n" thread-idx)
  (let ([ret (task)])
    ;  (printf "exec-task 1, ret: ~a\n" ret)
    ;  (read-line (current-input-port) 'any)
    (if (task 'dead?) ;check if coroutine is dead 
      (let ()
        ;      (printf "exec-task 2\n")
        #t ;task completed 
        )
      (let ()
        ;      (printf "exec-task 3\n")
        (if (> evals-left 0)
          (let ()
            ;          (printf "exec-task 4\n")
            (dp-thread-exec-task env thread-idx task (- evals-left 1))
            )
          (let ()
            ;          (printf "exec-task 5\n")
            (go env task) ;place task at the back of a queue
            #f)) ;task not yet completed  
        ))))


;; Eternal thread tail recursion of executing tasks
(define (dp-thread env thread-idx) 
  ;(printf "dp-thread 0\n")
  (let ([task (get-task env thread-idx)])
    ;  (printf "dp-thread 1\n")
    (if (equal? task #f)
      (let ()
        ;      (printf "dp-thread 2\n")
        (thread-suspend (current-thread))
        )
      (let ()
        ;      (printf "dp-thread 3\n")
        (dp-thread-exec-task ;execute the task we get
          env 
          thread-idx 
          task
          (get-dp-thread-continuous-eval-limit env))
        )))
  ;(printf "dp-thread 4\n")
  (dp-thread env thread-idx))


;; Thread startup coroutinetion
(define (dp-thread-start env)
  (let ([pid (current-thread)])
    (thread-suspend pid)
    (define thread-num 0)
    (for ([i (get-num-dp-threads env)])
         (when (equal? (get-dp-thread env i) pid) 
           (set! thread-num i)))
    (dp-thread env thread-num)))



;;;----------------------------------------------------------------------------
;;; Create Datapool
;;;---------------------------------------------------------------------------- 
;; Create a datapool environment. Setup worker threads and begin execution of 
;; the user defined main form. Returns datapool environment data vector needed
;; as the argument for management functions like (close-dp). To kick off 
;; datapool execution evaluate a (go env form). Form can be any arbitrary form,
;; *or* it can be a data object registered with the environment via the (data) 
;; macro below
(define (datapool num-threads dp-main)
  (let* ([eval-limit 10]
         [env (make-dp-data num-threads eval-limit)])
    (if (> (get-num-dp-threads env) 0)
      env
      #f))) ;Currently, must have 1 thread for datapool execution



;;;----------------------------------------------------------------------------
;;; classes & macros
;;;---------------------------------------------------------------------------- 
;; message class 
(define message%
  (class object% 
         (super-new)
         (init-field
           src
           type
           args)))


;; Data object interface
(define data-interface (interface () 
                                  delete
                                  deleted?))


;; Data object class
(define data%
  (class* 
    object% 
    (data-interface)
    (init-field 
      key
      env
      [deleted #f])
    (super-new)

    ;;,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    ;; Public
    ;;,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    ;; Check whether object is still valid
    (define/public
      (deleted?)
      deleted)

    ;; Set the current data object to the deleted state (won't accept any more
    (define/public (delete)
                   (set! deleted #t))

    ;;,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    ;; Private
    ;;,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    ;; Register a message callback
    (define/private 
      (register-message-handler msg-type callback-form)
      (if (not (deleted?))
        (let ([msg-handlers (hash-ref (get-dp-message-handlers env)
                                      msg-type
                                      (list))])
          (set-dp-message-handlers 
            env 
            msg-type 
            (append msg-handlers (list key callback-form)))
          #t)
        'deleted))

    ;; Send a message to connected handlers in the current datapool
    (define/private 
      (send-msg msg)
      (if (not (deleted?))
        (let ([handlers (hash-ref 
                          (get-dp-message-handlers) 
                          (get-field type msg))])
          (when (not (equal? handlers #f))
            (for 
              ([hpair handlers])
              (go (lambda ()
                    (let ([msg msg]
                          [h (cdr hpair)])
                      (h)))))))
        'deleted))

    ;; Create a message object 
    (define/private (message inp-type inp-args)
                    (make-object message% key inp-type inp-args))

    ;; Qt-esque connect macro
    (define-syntax receive-msg
      (syntax-rules ()
                    [(receive-msg src-obj-key msg-type dst-obj-key handler)
                     (if (not (deleted?))
                       (register-message-handler 
                         msg-type
                         (let ()
                           (define-coroutine
                             (handler-accessor)
                             (if (equal? (get-dp-data-object dst-obj-key) #f)
                               #f
                               (send 
                                 (get-dp-data-object dst-obj-key) 
                                 handler 
                                 (get-field args msg))))
                           handler-accessor))
                       'deleted)]))

    ;; Create task to asynchronously set an object's field. set-field! 
    ;; should be inherently threadsafe (just like normal set!). 
    ;; Ex:
    ;; (define-coroutine (gimme-3) (+ 1 2))
    ;; (go (set-datum! my-field gimme-3))
    (define-syntax set-datum!
      (syntax-rules ()
                    [(set-datum! field val) 
                     (if (not (deleted?))
                       (let ()
                         (define-coroutine
                           (set-handler)
                           (let ([obj (get-dp-data-object key)])
                             (if (equal? obj #f)
                               #f
                               (with-handlers 
                                 ([exn:fail?
                                    (let ()
                                      (semaphore-wait (get-dp-data-objects-sem))
                                      (let ([obj (get-dp-data-object key)])
                                        ;Protect against trying to set a deleted object
                                        (when (not obj #f)
                                          (set-field! 
                                            field 
                                            (get-dp-data-object key) 
                                            val)))
                                      (semaphore-post (get-dp-data-objects-sem)))])
                                 (display 
                                   "set-field failed with obj-key: ~a; field: ~a; val: 
                                   ~a" 
                                   key 
                                   field 
                                   val)))))
                             (go set-handler))
                           'deleted)]
                       [(set-datum! key field val)
                        (if (not (deleted?))
                          (let ()
                            (define-coroutine
                              (set-handler)
                              (let ([obj (get-dp-data-object key)])
                                (if (equal? obj #f)
                                  #f 
                                  (with-handlers 
                                    ([exn:fail?
                                       (let ()
                                         (semaphore-wait (get-dp-data-objects-sem))
                                         (let ([obj (get-dp-data-object key)])
                                           ;Protect against trying to set a deleted object
                                           (when (not obj #f)
                                             (set-field! 
                                               field 
                                               (get-dp-data-object key) 
                                               val)))
                                         (semaphore-post (get-dp-data-objects-sem)))])
                                    (display 
                                      "set-field failed with obj-key: ~a; field: ~a; val: 
                                      ~a" 
                                      key 
                                      field 
                                      val)))))
                                (go set-handler))
                              'deleted)]))))


;; Create and register a new data object with the datapool. Do not (emit), 
;; (connect), (set-datum!), or (go) in the object arguments or you may have a 
;; bad time.
(define-syntax data
  (syntax-rules ()
                [(data (name% env . params) body ...) ;for use at the parent level
                 (let ([new-key (gen-dp-data-obj-key)])
                   (hash-dp-data-object new-key 
                                        (let ([name%
                                                (class data%
                                                       (super-new)
                                                       (init-rest)
                                                       (define/public (run)
                                                                      body
                                                                      ...)
                                                       (get-datum key))])
                                          (make-object name% key env . params))))]
                [(data (name% . params) body ...) ;for use inside a data object
                 (let ([new-key (gen-dp-data-obj-key)])
                   (hash-dp-data-object new-key 
                                        (let ([name%
                                                (class data%
                                                       (super-new)
                                                       (init-rest)
                                                       (define/public (run)
                                                                      body
                                                                      ...)
                                                       (get-datum key))])
                                          (make-object name% key env . params))))]))


;; Data object destructor. Sets object to the 'deleted state, removes object 
;; (receive-msg) callbacks, removes the object from the object hash, and enqueues 
;; the now free object key onto a free floating key queue
(define (delete-data obj-key) 
  (let ([obj (get-dp-data-object obj-key)])
    (if (not (equal? obj #f)) ;if object exists
      (let ()
        ;Inform the data object it is deleted. This should stop it from 
        ;executing further (receive-msg), (send-msg), 
        (send obj delete)

        (define (remove-obj-callbacks hkey value) 
          ;Each h is a list of pairs in the form '(dst-obj-key dst-obj-callback)
          (for ([h (length value)])
               ;Return a list without handler pairs whose dst-obj-key 
               ;matches key
               (filter (not (equal? (car h) obj-key)))))

        ;Remove all callbacks to the data object mapped to key
        (semaphore-wait (get-dp-message-handlers-sem))
        (hash-for-each 
          (get-dp-message-handlers) 
          remove-obj-callbacks)
        (semaphore-post (get-dp-message-handlers-sem))

        ;Remove the data object from the hash
        (delete-dp-data-object obj-key)
        (add-free-dp-data-key obj-key)
        #t)
      #f))) 



;;;---------------------------------------------------------------------------- 
;; Run unit tests
;;;---------------------------------------------------------------------------- 
(define (run-unit-tests [print-result #t] [wait #f])
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
  (TEST-SECTION "test functions")
  (let ()
    (define x 3)
    (test-true? "defined? 1" (if (defined? x) #t #f) print-result wait)
    (test-true? "defined? 2" (if (defined? y) #f #t) print-result wait)
    (test-true? "test-true? 1" #t print-result wait)
    (test-true? "test-true? 2" (not #f) print-result wait)
    (test-equal? "test-equal? 3" 1 1 print-result wait)
    (test-equal? "test-equal? 4" "test" "test" print-result wait)
    (test-equal? "test-equal? 5" 'test 'test print-result wait))
  ;;**************************************


  ;;**************************************
  ;;TEST coroutine
  ;;--------------------------------------
  (TEST-SECTION "coroutines")
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
    (test-true? "new coroutine alive" (ct1 'alive?) print-result wait)
    (test-equal? "coroutine yields expected 1" (ct1) 1 print-result wait)
    (test-true? "coroutine alive" (ct1 'alive?) print-result wait)
    (test-equal? "coroutine yields expected 2" (ct1) 2 print-result wait)
    (test-true? "coroutine alive" (ct1 'alive?) print-result wait)
    (test-true? "coroutine returns #f" (not (ct1)) print-result wait)
    (test-true? "coroutine dead" (ct1 'dead?) print-result wait)

    (define-coroutine
      (co-test2 arg1 arg2)
      (yield arg1)
      (yield arg2)
      #f)

    (define ct2 (co-test2 "teststring" 'test))

    ;Test coroutine can be run with arguments
    (test-true? "new coroutine alive" (ct2 'alive?) print-result wait)
    (test-equal? "coroutine yields \"teststring\"" (ct2) "teststring" print-result wait)
    (test-true? "coroutine alive" (ct2 'alive?) print-result wait)
    (test-equal? "coroutine yields 'test" (ct2) 'test print-result wait)
    (test-true? "coroutine alive" (ct2 'alive?) print-result wait)
    (test-true? "coroutine returns #f" (not (ct2)) print-result wait)
    (test-true? "coroutine dead" (ct2 'dead?) print-result wait)

    (set! ct2 (co-test2 'test2 "teststring2"))

    ;Test coroutine can be rerun with new arguments
    (test-true? "new coroutine alive" (ct2 'alive?) print-result wait)
    (test-equal? "coroutine yields 'test" (ct2) 'test2 print-result wait)
    (test-true? "coroutine alive" (ct2 'alive?) print-result wait)
    (test-equal? "coroutine yields \"teststring2\"" (ct2) "teststring2" print-result wait)
    (test-true? "coroutine alive" (ct2 'alive?) print-result wait)
    (test-true? "coroutine" (not (ct2)) print-result wait)
    (test-true? "coroutine dead" (ct2 'dead?) print-result wait)

    ;Should *not* raise an exception due to the stateful (set!)
    (with-handlers ([exn:fail:user? (lambda (e) (test-fail e))])
                   (define-coroutine 
                     (co-test3 arg1)
                     (yield arg1)
                     (set! arg1 2)
                     (yield arg1))
                   (define ct3 (co-test3 1))
                   (test-equal? "new coroutine yields 1" (ct3) 1 print-result wait)
                   (test-equal? "coroutine yields changed argument (2)" (ct3) 2 print-result wait)
                   ;Final argument of coroutine is a (yield) so we have to run 
                   ;again to hit the end (even though there are no forms left 
                   ;to evaluate)
                   (test-equal? "coroutine returns 2 (previous return value), because the last form is a (yield) so the coroutine is not technically 'dead yet" (ct3) 2 print-result wait)
                   (test-true? "coroutine dead" (ct3 'dead?) print-result wait)) 

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
    (test-true? "new coroutine alive" (ct4 'alive?) print-result wait)
    (test-equal? "recursive function in coroutine yields expected value" (ct4) 3 print-result wait)
    (test-equal? "recursive function in coroutine yields expected value" (ct4) 2 print-result wait)
    (test-equal? "recursive function in coroutine yields expected value" (ct4) 1 print-result wait)
    (test-equal? "recursive function in coroutine returns expected value" (ct4) 0 print-result wait)
    (test-true? "coroutine dead" (ct4 'dead?) print-result wait)) 
  ;;************************************** 


  ;;**************************************
  ;;TEST channel ch-get ch-put
  ;;--------------------------------------
  (TEST-SECTION "channels")
  (let ([ch (channel)])
    (test-true? "channel creation" (async-channel? ch) print-result wait))

  (let ([ch (channel)])
    (async-channel-put ch "teststring")
    (let ([ret (ch-get ch #f)])
      (test-equal? "ch-get can get a string" ret "teststring" print-result wait)))

  (let ([ch (channel)])
    (async-channel-put ch "teststring2")
    (let ([ret (ch-get ch)])
      (test-equal? "ch-get can get a different string" ret "teststring2" print-result wait)))

  (let ([ch (channel)])
    (ch-put ch "teststring")
    (let ([ret (ch-get ch #f)])
      (test-equal? "ch-put a string" ret "teststring" print-result wait)))

  (let ([ch (channel)])
    (ch-put ch "teststring2")
    (let ([ret (ch-get ch)])
      (test-equal? "ch-put another string" ret "teststring2" print-result wait)))
  ;;**************************************


  ;;**************************************
  ;;TEST make-dp-data 
  ;;     get-dp-data 
  ;;     close-dp
  ;;--------------------------------------
  (TEST-SECTION "datapool data constructor, getter, and destructor functions")
  ;Make a datapool
  (let* ([num-threads 2]
         [eval-limit 10]
         [env (make-dp-data num-threads eval-limit)])

    ;Verify threads exist
    (for ([i num-threads])
         (test-true? "(and make-dp-data get-dp-data) verify threads exist" 
                     (thread? 
                       (vector-ref (vector-ref (vector-ref (get-dp-data env) 0) i) 0)) print-result wait))

    ;Verify the threads are alive
    (for ([i num-threads])
         (test-true? "(and make-dp-data get-dp-data) verify threads are alive" (not (thread-dead? 
                                                                                      (vector-ref
                                                                                        (vector-ref 
                                                                                          (vector-ref (get-dp-data env) 0) 
                                                                                          i) 0))) print-result wait))

    ;Verify task queues exist
    (for ([i num-threads])
         (test-true? "(and make-dp-data get-dp-data) verify task queues exist" 
                     (queue? 
                       (vector-ref (vector-ref (vector-ref (get-dp-data env) 0) i) 1)) print-result wait))

    ;Verify task queue semaphores exist
    (for ([i num-threads])
         (test-true? "(and make-dp-data get-dp-data) verify task queue semaphores exist" 
                     (semaphore? 
                       (vector-ref (vector-ref (vector-ref (get-dp-data env) 0) i) 2)) print-result wait))

    ;Verify hash table of data objects exists
    (test-true? "(and make-dp-data get-dp-data) verify hash table of data objects exists" 
                (hash? 
                  (vector-ref (vector-ref (get-dp-data env) 1) 0)) print-result wait)

    ;Verify hash table of message handlers exists
    (test-true? "(and make-dp-data get-dp-data) verify message handler hash table exists" 
                (hash? 
                  (vector-ref (vector-ref (get-dp-data env) 1) 1)) print-result wait)

    ;Verify hash table semaphore exists
    (test-true? "(and make-dp-data get-dp-data) verify data object hash table semaphore exists" 
                (semaphore? 
                  (vector-ref (vector-ref (get-dp-data env) 1) 2)) print-result wait)

    ;Verify hash table semaphore exists
    (test-true? "(and make-dp-data get-dp-data) verify message handlers hash table semaphore exists" 
                (semaphore? 
                  (vector-ref (vector-ref (get-dp-data env) 1) 3)) print-result wait)

    ;Verify parent->dp channel exists
    (test-true? "(and make-dp-data get-dp-data) verify parent->dp channel exists" 
                (async-channel? 
                  (vector-ref (vector-ref (get-dp-data env) 2) 0)) print-result wait)

    ;Verify dp->parent channel exists
    (test-true? "(and make-dp-data get-dp-data) verify dp->parent channel exists" 
                (async-channel? 
                  (vector-ref (vector-ref (get-dp-data env) 2) 1)) print-result wait)

    ;Verify dp->parent channel exists
    (test-true? "(and make-dp-data get-dp-data) verify dp->parent channel semaphore exists" 
                (semaphore? 
                  (vector-ref (vector-ref (get-dp-data env) 2) 2)) print-result wait)

    ;Verify data object key source variable exists
    (test-true? "(and make-dp-data get-dp-data) verify data object key source variable exists" 
                (number? 
                  (vector-ref (vector-ref (get-dp-data env) 3) 0)) print-result wait)

    ;Verify data object freed key queue exists
    (test-true? "(and make-dp-data get-dp-data) verify data object freed key queue exists" 
                (queue? 
                  (vector-ref (vector-ref (get-dp-data env) 3) 1)) print-result wait)

    ;Verify coroutine evaluation rule exists
    (test-true? "(and make-dp-data get-dp-data) verify coroutine evaluation rule exists" 
                (number? 
                  (vector-ref (vector-ref (get-dp-data env) 4) 0)) print-result wait)

    ;Verify we can kill the datapool environment
    (close-dp env)

    (for ([i num-threads])
         (let ([o (open-output-string)])
           (fprintf o "close-dp thread ~a" i)
           (test-true? (get-output-string o) (thread-dead? 
                                               (vector-ref 
                                                 (vector-ref 
                                                   (vector-ref (get-dp-data env) 0) i) 0)) print-result wait))))
  ;;**************************************


  ;;**************************************
  ;;TEST get-datapool-input-channel 
  ;;     get-num-dp-threads 
  ;;     get-dp-thread 
  ;;     get-dp-queue 
  ;;     get-dp-queue-sem 
  ;;     get-dp-data-objects 
  ;;     get-dp-data-objects-sem 
  ;;     gen-dp-data-obj-key 
  ;;     get-data-obj-key-src 
  ;;     set-data-obj-key-src 
  ;;     get-data-obj-free-key-q
  ;;     get-dp-thread-continuous-eval-limit
  ;;--------------------------------------
  (TEST-SECTION "datapool getters & setters")
  (let* ([num-threads 2]
         [eval-limit 10]
         [env (make-dp-data num-threads eval-limit)])

    ;verify that parent->dp channel exists
    (test-true? "get-datapool-input-channel verify that parent->dp channel exists" (async-channel? 
                                                                                     (get-datapool-input-channel env)) print-result wait)

    ;verify correct num of threads exist 
    (test-equal? "get-num-dp-threads verify correct num of threads exist" (get-num-dp-threads env) 2 print-result wait)

    ;verify threads exist 
    (test-true? "get-dp-thread verify threads exist 1" (thread? (get-dp-thread env 0)) print-result wait)
    (test-true? "get-dp-thread verify threads exist 2" (thread? (get-dp-thread env 1)) print-result wait)

    ;verify thread task queues exist
    (test-true? "get-dp-queue verify thread task queues exist 1" (queue? (get-dp-queue env 0)) print-result wait)
    (test-true? "get-dp-queue verify thread task queues exist 2" (queue? (get-dp-queue env 1)) print-result wait)

    ;verify thread task queues semaphores exist
    (test-true? "get-dp-queue-sem verify thread task queues semaphores exist 1" (semaphore? (get-dp-queue-sem env 0)) print-result wait)
    (test-true? "get-dp-queue-sem verify thread task queues semaphores exist 2" (semaphore? (get-dp-queue-sem env 1)) print-result wait)

    ;verify data object hash exists
    (test-true? "get-dp-data-objects verify data object hash exists" (hash? (get-dp-data-objects env)) print-result wait)
    (test-true? "get-dp-data-objects-sem verify data object hash semaphore exists" (semaphore? (get-dp-data-objects-sem env)) print-result wait)

    ;verify key generation
    (test-equal? "gen-dp-data-obj-key verify key generation 1" (gen-dp-data-obj-key env) 0 print-result wait)
    (test-equal? "gen-dp-data-obj-key verify key generation 2" (gen-dp-data-obj-key env) 1 print-result wait)
    (test-equal? "gen-dp-data-obj-key verify key generation 3" (gen-dp-data-obj-key env) 2 print-result wait)
    (test-equal? "gen-dp-data-obj-key verify key generation 4" (gen-dp-data-obj-key env) 3 print-result wait)

    ;verify can get data object key source number  
    (test-equal? "get-data-obj-key-src" (get-data-obj-key-src env) 4 print-result wait)
    (test-equal? "get-data-obj-key-src" (get-data-obj-key-src env) 4 print-result wait)

    ;verify can set data object key source number (?? should never do this?)
    (set-data-obj-key-src env 2)
    (test-equal? "get-data-obj-key-src" (get-data-obj-key-src env) 2 print-result wait) 

    ;verify get-data-obj-free-key-q
    (test-true? "get-data-object-free-key-q" (queue? (get-data-obj-free-key-q env)) print-result wait)

    ;verify get-dp-thread-continuous-eval-limit
    (test-true? "get-dp-thread-continuous-eval-limit" (number? (get-dp-thread-continuous-eval-limit env)) print-result wait)

    (close-dp env))
  ;;**************************************

  ;;**************************************
  ;;TEST get-min-dp-q-idx 
  ;;     get-max-dp-q-idx 
  ;;     go
  ;;--------------------------------------
  (TEST-SECTION "task queue getters & setters")
  (let* ([num-threads 2]
         [eval-limit 10]
         [env (make-dp-data num-threads eval-limit)])

    ;;Arbitrary coroutinetion to execute
    (define (test-task) #t)

    ;;Test defaults
    ;q0-size: 0, q1-size: 0
    (test-equal? "get-min-dp-q-idx 1" (get-min-dp-q-idx env) 0 print-result wait)
    (test-equal? "get-max-dp-q-idx 1" (get-max-dp-q-idx env) 0 print-result wait)
    (test-equal? "q-len idx 0" (queue-length (get-dp-queue env 0)) 0 print-result wait)
    (test-equal? "q-len idx 1" (queue-length (get-dp-queue env 1)) 0 print-result wait)

    (enqueue! (get-dp-queue env 0) test-task)

    ;q0-size: 1, q1-size: 0
    (test-equal? "get-min-dp-q-idx 2" (get-min-dp-q-idx env) 1 print-result wait)
    (test-equal? "get-max-dp-q-idx 2" (get-max-dp-q-idx env) 0 print-result wait)
    (test-equal? "q-len idx 0" (queue-length (get-dp-queue env 0)) 1 print-result wait)
    (test-equal? "q-len idx 1" (queue-length (get-dp-queue env 1)) 0 print-result wait)

    (enqueue! (get-dp-queue env 1) test-task)

    ;q0-size: 1, q1-size: 1
    (test-equal? "get-min-dp-q-idx 3" (get-min-dp-q-idx env) 0 print-result wait)
    (test-equal? "get-max-dp-q-idx 3" (get-max-dp-q-idx env) 0 print-result wait)
    (test-equal? "q-len idx 0" (queue-length (get-dp-queue env 0)) 1 print-result wait)
    (test-equal? "q-len idx 1" (queue-length (get-dp-queue env 1)) 1 print-result wait)

    (enqueue! (get-dp-queue env 1) test-task)

    ;q0-size: 1, q1-size: 2
    (test-equal? "get-min-dp-q-idx 4" (get-min-dp-q-idx env) 0 print-result wait)
    (test-equal? "get-max-dp-q-idx 4" (get-max-dp-q-idx env) 1 print-result wait)
    (test-equal? "q-len idx 0" (queue-length (get-dp-queue env 0)) 1 print-result wait)
    (test-equal? "q-len idx 1" (queue-length (get-dp-queue env 1)) 2 print-result wait)

    (close-dp env))
  ;;**************************************


  ;;**************************************
  ;;TEST hash-dp-data-object
  ;;     get-dp-data-object
  ;;     delete-dp-data-object
  ;;--------------------------------------
  (TEST-SECTION "manage data objects")
  (let* ([num-threads 2]
         [eval-limit 10]
         [env (make-dp-data num-threads eval-limit)])

    (define test-class%
      (class object% (super-new)
             (define/public (get-3)
                            3)))

    (define test-object (make-object test-class%))

    (test-equal? "get-dp-data-objects 1" (hash-count (get-dp-data-objects env)) 0 print-result wait)
    (test-true? "hash-dp-data-objects 1" (hash-dp-data-object env 0 test-object) print-result wait)
    (test-true? "hash-dp-data-objects 2" (not (hash-dp-data-object env 0 test-object)) print-result wait)

    (test-equal? "get-dp-data-object 1" (send (get-dp-data-object env 0) get-3) 3 print-result wait)
    (test-true? "get-dp-data-object 2" (not (get-dp-data-object env 1)) print-result wait)

    (test-true? "delete-dp-data-object 1" (delete-dp-data-object env 0) print-result wait)
    (test-true? "get-dp-data-object 1" (not (get-dp-data-object env 0)) print-result wait)
    (close-dp env))
  ;;**************************************


  ;;**************************************
  ;;TEST get-dp-message-handlers
  ;;     get-dp-message-handlers-sem
  ;;     set-dp-message-handlers
  ;;--------------------------------------
  (TEST-SECTION "manage message handlers")
  (let* ([num-threads 2]
         [eval-limit 10]
         [env (make-dp-data num-threads eval-limit)])
    (test-true? "get-dp-message-handlers" (hash? (get-dp-message-handlers env)) print-result wait)
    (test-true? "get-dp-message-handlers-sem" (semaphore? (get-dp-message-handlers-sem env)) print-result wait)

    (define test-type 'test-type)
    (define callback-form (lambda () 1))
    (test-equal? "callback check" (callback-form) 1 print-result wait)

    ;Probably should rework this coroutineion to be simpler. However, it's not 
    ;exposed by the module and should only be used internally so it's ppobably 
    ;fine for now
    (set-dp-message-handlers env test-type (list callback-form))

    (test-equal? "(and set-dp-message-handlers get-dp-message-handlers) 1" 
                 ((car (hash-ref (get-dp-message-handlers env) test-type)))
                 (callback-form) 
                 print-result 
                 wait)

    (define callback-form-2 (lambda () 2))
    (test-equal? "callback check 2" (callback-form-2) 2 print-result wait)

    (set-dp-message-handlers 
      env 
      test-type 
      (append 
        (hash-ref (get-dp-message-handlers env) test-type) 
        (list callback-form-2)))

    (test-equal? "(and set-dp-message-handlers get-dp-message-handlers) 2" 
                 (hash-ref (get-dp-message-handlers env) test-type) 
                 (list callback-form callback-form-2) 
                 print-result 
                 wait)
    (let ([pre-handlers (list callback-form callback-form-2)]
          [post-handlers (hash-ref (get-dp-message-handlers env) test-type) ])
      (for ([i (length pre-handlers)])
           (let ([str 
                   (let ([o (open-output-string)])
                     (fprintf o "compare callback results ~a" i)
                     (get-output-string o))])
             (test-equal? str
                          ((list-ref pre-handlers i))
                          ((list-ref post-handlers i))
                          print-result
                          wait))))
    (close-dp env))
  ;;**************************************

  ;;**************************************
  ;;TEST get-datapool-output-channel
  ;;     get-dp-parent-ch-sem
  ;;     send-to-datapool-output-channel 
  ;;--------------------------------------
  (TEST-SECTION "datapool channel functions")
  (let* ([num-threads 2]
         [eval-limit 10]
         [env (make-dp-data num-threads eval-limit)])
    (test-true? "get-datapool-output-channel" (async-channel? (get-datapool-output-channel env)) print-result wait)
    (test-true? "get-dp-parent-ch-sem" (semaphore? (get-dp-parent-ch-sem env)) print-result wait)

    (ch-put (get-datapool-output-channel env) #t)

    (test-true? "get data from channel" (ch-get (get-datapool-output-channel env)) print-result wait)

    (ch-put (get-datapool-output-channel env) "test-string")

    (test-equal? "get string from channel" (ch-get (get-datapool-output-channel env)) "test-string" print-result wait)

    (send-to-datapool-output-channel env #t)

    (test-true? "get-datapool-input-channel" (async-channel? (get-datapool-input-channel env)) print-result wait)
    (test-true? "(and send-to-datapool-output-channel get-datapool-input-channel) with a boolean" 
                (ch-get (get-datapool-output-channel env)) 
                print-result 
                wait)

    (send-to-datapool-output-channel env "test-string2")
    (test-equal? "(and send-to-datapool-output-channel get-datapool-input-channel) with a test string" 
                 (ch-get (get-datapool-output-channel env)) 
                 "test-string2" 
                 print-result 
                 wait)
    (close-dp env))
  ;;**************************************

  ;;**************************************
  ;;TEST get-task-q-idx
  ;;     get-task
  ;;     dp-thread-exec-task
  ;;     dp-thread
  ;;     dp-thread-start
  ;;     go ;from within datapool
  ;;     go ;from parent
  ;;--------------------------------------
  (TEST-SECTION "datapool thread internal functions")
  (let* ([num-threads 2]
         [eval-limit 10]
         [env (make-dp-data num-threads eval-limit)])
    (define-coroutine (test-task-co)
                      3)
    (define test-task (test-task-co))

    (enqueue! (get-dp-queue env 0) test-task)

    (test-equal? "get-task-q-idx" (get-task-q-idx env 0) 0 print-result wait)

    (dequeue! (get-dp-queue env 0))
    (enqueue! (get-dp-queue env 1) test-task)

    (test-equal? "get-task-q-idx 0" (get-task-q-idx env 0) 1 print-result wait)
    (test-equal? "get-task-q-idx 1" (get-task-q-idx env 1) 1 print-result wait)

    (enqueue! (get-dp-queue env 1) test-task)

    (test-equal? "get-task-q-idx 0" (get-task-q-idx env 0) 1 print-result wait)
    (test-equal? "get-task-q-idx 1" (get-task-q-idx env 1) 1 print-result wait)

    (enqueue! (get-dp-queue env 0) test-task)

    (test-equal? "get-task-q-idx 0" (get-task-q-idx env 0) 0 print-result wait)
    (test-equal? "get-task-q-idx 1" (get-task-q-idx env 1) 1 print-result wait)

    (let ([len-0 (queue-length (get-dp-queue env 0))]
          [task (get-task env 0)])
      (test-true? "get-task" (> len-0 (queue-length (get-dp-queue env 0))) print-result wait))

    (define t-thread
      (thread 
        (thunk 

          ;coroutine coroutine that evaluates infinitely
          (define-coroutine 
            (eval-infinity) 
            (define ;best to internally define a looping function, due to the added complexity
              ;of dealing with a coroutine initially returning a function that is waiting
              ;to be run. Tail returning is confusing in this situation (not even sure
              ;if directly possible?), better to create an internal function to tail 
              ;recurse and use the (yield) functionality given by the coroutine
              (intern-func) 
              (yield 2)
              (intern-func))
            (intern-func))

          ;Should evaluate 10 times then return #f
          (test-true? "dp-thread-exec-task" (not (dp-thread-exec-task env 0 (eval-infinity) 10)) print-result wait))))

    (kill-thread t-thread)

    (sleep 0.1)
    ;we're not using an infinitely recursing thread here, it should be dead
    (test-true? "verify test thread is dead" (thread-dead? t-thread) print-result wait)
    (test-true? "verify test thread is not running" (not (thread-running? t-thread)) print-result wait)

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
         (test-true? "Check if dp thread is not dead" (not (thread-dead? (get-dp-thread env 1))) print-result wait)
         (test-true? "Check if dp thread is not running" (not (thread-running? (get-dp-thread env 1))) print-result wait)
         (test-equal? "Verify task queue is empty" (queue-length (get-dp-queue env 0)) 0 print-result wait))


    (printf "\n")
    (for ([i (length inp-vals)])
         (let ([val (list-ref inp-vals i)]
               [ret (ch-get ch #f)])
           (test-equal? "Did expected val get placed in channel" ret val print-result wait)))


    (define inp-vals2 (list 'test3 'test4 #t "teststring2"))
    (go env (test-task-co2 ch inp-vals2))
    (sleep 0.1)

    (for ([i num-threads])
         (printf "\n--- tests for thread ~a ---\n" i)
         (test-true? "Check if dp thread is dead" (not (thread-dead? (get-dp-thread env 1))) print-result wait)
         (test-true? "Check if dp thread is actively running" (not (thread-running? (get-dp-thread env 1))) print-result wait)
         (test-equal? "Verify task queue is empty" (queue-length (get-dp-queue env 0)) 0 print-result wait))

    (printf "\n")
    (for ([i (length inp-vals2)])
         (let ([val (list-ref inp-vals2 i)]
               [ret (ch-get ch #f)])
           (test-equal? "Did expected val get placed in channel" ret val print-result wait)))
    (close-dp env))

  ;;**************************************

  ;;**************************************
  ;;TEST go ;stress test
  ;;--------------------------------------
  (TEST-SECTION "go stress test")
  (let* ([num-threads 8]
         [eval-limit 10]
         [env (make-dp-data num-threads eval-limit)])

    (sleep 0.1)

    ;verify threads exist 
    (test-true? "get-dp-thread verify threads exist 1" (thread? (get-dp-thread env 0)) print-result wait)
    (test-true? "get-dp-thread verify threads exist 2" (thread? (get-dp-thread env 1)) print-result wait)
    (test-true? "Check if dp thread is not dead" (not (thread-dead? (get-dp-thread env 0))) print-result wait)
    (test-true? "Check if dp thread is not dead" (not (thread-dead? (get-dp-thread env 1))) print-result wait)
    (test-true? "Check if dp thread is not running" (not (thread-running? (get-dp-thread env 0))) print-result wait)
    (test-true? "Check if dp thread is not running" (not (thread-running? (get-dp-thread env 1))) print-result wait)

    (define-coroutine 
      (eval-x-times env inp-x)
      (define (in-loop x)
        (if (equal? x 0)
          (ch-put (get-datapool-output-channel env) inp-x)
          (let ()
            ;(yield x)
            (in-loop (- x 1)))))
      (in-loop inp-x))

    (let ([x 200]
          [x2 10])
      (test-equal? "" (queue-length (get-dp-queue env 0)) 0 print-result wait)
      (test-equal? "" (queue-length (get-dp-queue env 1)) 0 print-result wait)

      (for ([i x2])
           (enqueue! (get-dp-queue env 0) (eval-x-times env i))
           (enqueue! (get-dp-queue env 1) (eval-x-times env i)))

      (test-equal? "" (queue-length (get-dp-queue env 0)) x2 print-result wait)
      (test-equal? "" (queue-length (get-dp-queue env 1)) x2 print-result wait)

      (go env (eval-x-times env x))

      (sleep 1.0)

      (test-equal? "" (queue-length (get-dp-queue env 0)) 0 print-result wait)
      (test-equal? "" (queue-length (get-dp-queue env 1)) 0 print-result wait)

      (for ([i x])
           (go env (eval-x-times env x)))

      (let ([start-time (current-inexact-milliseconds)])
        (for ([i x])
             (ch-get (get-datapool-output-channel env)))
        (printf "Benchmark time (milli) for ~a evaluations on ~a threads: ~a\n" (* x x) num-threads (- (current-inexact-milliseconds) start-time)))

      (sleep 0.1)

      (test-equal? "" (queue-length (get-dp-queue env 0)) 0 print-result wait)
      (test-equal? "" (queue-length (get-dp-queue env 1)) 0 print-result wait))
    (close-dp env))
  ;;**************************************

  ;;**************************************
  ;;TEST datapool
  ;;--------------------------------------
  #|
  (let ()
    (define dp (datapool 2))

    (define-coroutine (main env) 
                      (let ([parent (get-datapool-output-channel env)])
                        (ch-put parent "test string")))

    (go dp (main dp))
    (sleep 0.1)

    (test-equal? "" (ch-get (get-datapool-input-channel dp)) "test string" print-result wait)
    (close-dp dp))
  |#
  ;;**************************************

  ;;**************************************
  ;;TEST message%
  ;;--------------------------------------
  ;(let ([num-threads 2])
  ;(let 
  ;      ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
  ;      [*datapool-environment-data* (make-dp-data num-threads)]
  ;      [*data-obj-key-src* 0])
  ;  ))
  ;;**************************************

  ;;**************************************
  ;;TEST data%
  ;;     register-message-handler
  ;;     send
  ;;     deleted?
  ;;     delete
  ;;     message
  ;;     receive
  ;;     set-datum
  ;;--------------------------------------
  ;(let ([num-threads 2])
  ;(let 
  ;      ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
  ;      [*datapool-environment-data* (make-dp-data num-threads)]
  ;      [*data-obj-key-src* 0])
  ;  ))
  ;;**************************************

  ;;**************************************
  ;;TEST data
  ;;     delete-data
  ;;--------------------------------------
  ;(let ([num-threads 2])
  ;(let 
  ;      ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
  ;      [*datapool-environment-data* (make-dp-data num-threads)]
  ;      [*data-obj-key-src* 0])
  ;  ))
  ;;**************************************



  ;;;---------------------------------------------------------------------------- 
  ;;; Feature Tests
  ;;;---------------------------------------------------------------------------- 
  ;TODO figure out how to get argv & argc 
  ;(define dp1 (datapool 4 '(main argv argc)))
  ;(let ([ch (get-datapool-input-channel dp1)]) 
  ;(ch-get ch))


  ;;;---------------------------------------------------------------------------- 
  ;;; Closing Analysis
  ;;;---------------------------------------------------------------------------- 
  (print-test-section-report))
