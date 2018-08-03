#lang racket
(require racket/base
         racket/class
         racket/async-channel
         data/queue
         delay-pure) 

;;;----------------------------------------------------------------------------
;;; Forward Definitions
;;;----------------------------------------------------------------------------
(define (get-dp-thread-pid idx) (#f))
(define (get-dp-thread-pid idx) #f)
(define (get-dp-queue idx) #f)
(define (get-dp-queue-sem idx) #f)
(define (get-dp-message-handlers) #f)
(define (get-dp-message-handlers-sem) #f)
(define (get-dp-parent-channel) #f)
(define (set-dp-message-handlers handlers) #f)
(define (get-max-dp-q-idx) #f)
(define (get-min-dp-q-idx) #f)
(define (get-task-q-idx thread-idx) #f)
(define (get-task thread-idx) #f)
(define (go form) #f)
(define (dp-thread-eval-task thread-idx task evals-left) #f)
(define (dp-thread thread-idx) #f)
(define (dp-thread-start) (thread-suspend (current-thread)))




;;;----------------------------------------------------------------------------
;;; Test Functions (to run tests (define *run-3-tests #t) before loading)
;;;---------------------------------------------------------------------------- 
;; Return #t if the quoted form returns #t, else #f 
(define (test-true form)
  (if (eqv? form #t)
      (begin
        (printf "pass: ~a == #t" form)
        #t)
      (begin
        (printf "FAIL: ~a == #f" form)
        #f)))


;; Return #t if quoted forms return an equal value, else #f
(define (test-equal form-a form-b)
  (if (eqv? form-a form-b)
      (begin
        (printf "pass: ~a == ~a" form-a form-b)
        #t)
      (begin
        (printf "FAIL: ~a == ~a" form-a form-b)
        #f))) 


;; Custom test fail
(define (test-fail form)
  (printf "FAIL: ~a" form))


;; Custom test pass
(define (test-pass form)
  (printf "pass: ~a" form))


;; Return #t if the given identifier is defined, else #f
(define-syntax (defined? stx)
  (syntax-case stx ()
               [(_ id)
                (with-syntax ([v (identifier-binding #'id)])
                             #''v)]))



;;;----------------------------------------------------------------------------
;;; Pure/Stateless Coroutines
;;;----------------------------------------------------------------------------  
;;**************************************
;; Forward define for yield 
(define (yield val) val)


;; Coroutine definition  
(define (make-coroutine procedure)
  (define last-return values)
  (define last-value #f)
  (define (last-continuation _) 
    (let ((result (procedure yield))) 
      (last-return result)))

  (define (yield value)
    (call/cc (lambda (continuation)
               (set! last-continuation continuation)
               (set! last-value value)
               (last-return value))))

  (lambda args
    (call/cc (lambda (return)
               (set! last-return return)
               (if (null? args)
                   (last-continuation last-value)
                   (apply last-continuation args))))))


(define-syntax (define-coroutine stx)
  (syntax-case stx ()
               ((_ (name . args) . body )
                #`(define (name . args)
                    (make-generator 
                      (lambda (#,(datum->syntax stx 'yield))
                        . body))))))

;;TEST
;; Run tests for co-routines
(when (defined? *run-3-tests*)
  (define-coroutine
    (co-test1) 
    (yield 1)
    (yield 2)
    #f)


  ;; Test the coroutine runs correctly
  (test-equal (co-test1) 1)
  (test-equal (co-test1) 2)
  (test-true (not (co-test1)))

  (define-coroutine
    (co-test2 arg1 arg2)
    (yield arg1)
    (yield arg2)
    #f)

  ;; Test coroutine can be run with arguments
  (test-equal (co-test2 "teststring" 'test)  "teststring")
  (test-equal (co-test2)  'test)
  (test-true (not co-test2))

  ;; Test coroutine can be rerun with new arguments
  (test-equal (co-test2 'test2 "teststring2")  'test2)
  (test-equal (co-test2)  "teststring2")
  (test-true (not co-test2)))
;;**************************************


;;**************************************
;; define a coroutine of a pure stateless coroutine. These should always be 
;; safe when executed asynchronously with a (go) call. The current 'coroutine' 
;; portion of the definition *is* stateful, only the provide form is 
;; guaranteed to be stateless
(define-syntax-rule (func (name . params) body ...)
                    (eval (if (pure/stateless (define (name . params) body ...))
                              '(define-coroutine (name . params)
                                                 (begin
                                                   (yield 'alive) 
                                                   body
                                                   ...
                                                   #f))
                              (raise 
                                (make-exn:fail:user
                                  '("Function is not pure: " 
                                    (define (name . params) body ...)))))))

;;TEST
;; Tests for (func) definitions
(when (defined? *run-3-tests*)
  (func
    (co-test1) 
    (yield 1)
    (yield 2)
    #f)

  ;; Test the coroutine runs correctly
  (test-equal (co-test1) 1)
  (test-equal (co-test1) 2)
  (test-true (not (co-test1)))

  (func
    (co-test2 arg1 arg2)
    (yield arg1)
    (yield arg2)
    #f)

  ;; Test coroutine can be run with arguments
  (test-equal (co-test2 "teststring" 'test)  "teststring")
  (test-equal (co-test2)  'test)
  (test-true (not co-test2))

  ;; Test coroutine can be rerun with new arguments
  (test-equal (co-test2 'test2 "teststring2")  'test2)
  (test-equal (co-test2)  "teststring2")
  (test-true (not co-test2))

  ;; Should raise an exception due to the stateful (set!)
  (with-handlers ([exn:fail:user? (lambda (e) (test-pass e))])
                 (func 
                   (co-test3 arg1)
                   (yield arg1)
                   (set! arg1 2)
                   (yield arg2))
                 (test-fail (co-test3 1))))
;;**************************************


;;**************************************
;; DO *NOT* USE IF YOU DON'T HAVE TO FOR PERFORMANCE REASONS
;; Define a coroutine with no purity/state checks. (func) can be slow because 
;; it fully expands the form checking for purity/statelessness. This rule skips
;; that check, which makes it *dangerous* when used in the multithreaded (go)
;; environment. Therefore, even if you use this function you should still 
;; validate its purity, manually if necessary.
(define-syntax-rule (func! (name . params) body ...)
                    (define-coroutine (name . params)
                                      (begin
                                        (yield 'alive) 
                                        body
                                        ...
                                        #f)))

;;TEST
;; Tests for (func!) definitions
(when (defined? *run-3-tests*)
  (func!
    (co-test1) 
    (yield 1)
    (yield 2)
    #f)

  ;; Test the coroutine runs correctly
  (test-equal (co-test1) 1)
  (test-equal (co-test1) 2)
  (test-true (not (co-test1)))

  (func!
    (co-test2 arg1 arg2)
    (yield arg1)
    (yield arg2)
    #f)

  ;; Test coroutine can be run with arguments
  (test-equal (co-test2 "teststring" 'test)  "teststring")
  (test-equal (co-test2)  'test)
  (test-true (not co-test2))

  ;; Test coroutine can be rerun with new arguments
  (test-equal (co-test2 'test2 "teststring2")  'test2)
  (test-equal (co-test2)  "teststring2")
  (test-true (not co-test2))

  ;; Should *not* raise an exception due to the stateful (set!)
  (with-handlers ([exn:fail:user? (lambda (e) (test-fail e))])
                 (func! 
                   (co-test3 arg1)
                   (yield arg1)
                   (set! arg1 2)
                   (yield arg2))
                 (test-equal (co-test3 1) 1)
                 (test-equal (co-test3) 2)
                 (test-true (not (co-test3)))))
;;**************************************



;;;----------------------------------------------------------------------------
;;;basic channel functions
;;;----------------------------------------------------------------------------
;;**************************************
;;create an async channel, no size limit by default
(define (channel [size #f]) (make-async-channel size))

;;TEST
(when (defined? *run-3-tests*)
  (let ([ch (channel)])
    (test-true (async-channel? ch))))
;;**************************************


;;**************************************
;;channel get, blocks by default
(define (<- ch [block #t])
  (if block
      (async-channel-get ch)
      (async-channel-try-get ch)))

;;TEST
(when (defined? *run-3-tests*)
  (let ([ch (channel)])
    (async-channel-put ch "teststring")
    (let ([ret (<- ch #f)])
      (test-equal ret "teststring")))

  (let ([ch (channel)])
    (async-channel-put ch "teststring2")
    (let ([ret (<- ch)])
      (test-equal ret "teststring2"))))
;;**************************************


;;**************************************
;;channel put
(define (-> ch item)
  (async-channel-put ch item))

;;TEST
(when (defined? *run-3-tests*)
  (let ([ch (channel)])
    (-> ch "teststring")
    (let ([ret (<- ch #f)])
      (test-equal ret "teststring")))

  (let ([ch (channel)])
    (-> ch "teststring2")
    (let ([ret (<- ch)])
      (test-equal ret "teststring2"))))
;;**************************************



;;;--------------------------------------------------------------------------
;;; Global forward declarations and globals
;;;--------------------------------------------------------------------------
;; Number of threads 
(define *num-dp-threads* #f)

;; Current datapool environment data
(define *datapool-environment-data* #f)

;; Number of times a coroutine is to be evaluated in a row before swapping tasks
(define *dp-thread-continuous-eval-limit* 10) 



;;;--------------------------------------------------------------------------
;;; Public Datapool Functions
;;;--------------------------------------------------------------------------
;;**************************************
;;create datapool data 
(define (make-dp-data num-threads) 
  (vector 
    ;thread id's, queues, and semaphores
    (make-vector num-threads '((thread (thunk (dp-thread-start)))
                               (make-queue) 
                               (make-semaphore 1))) 
    '((make-hash) (make-semaphore 1));hash table of lists of message handlers
    '((channel #f) (channel #f)))) ;channel to datapool parent scope

;; Return the datapool's data
(define (get-dp-data)
  *datapool-environment-data*)


;;kill all threads in a datapool
(define (close-dp dp-data)
  (for ([i (vector-length (vector-ref dp-data 0))])
       (kill-thread (car (vector-ref (vector-ref dp-data 0) i)))))

;;TEST
(when (defined? *run-3-tests*)
  ;; Sanity test
  (test-equal (get-dp-data) #f)

  ;; Make a datapool
  (let ([num-threads 2]
        [*datapool-environment-data* (make-dp-data num-threads)])

    ;; Verify threads exist
    (for ([i num-threads])
         (test-true 
           (thread? 
             (car (vector-ref (vector-ref (get-dp-data) 0) i)))))

    ;; Verify the threads are alive
    (for ([i num-threads])
         (test-true (not (thread-dead? 
                           (car 
                             (vector-ref 
                               (vector-ref (get-dp-data) 0) 
                               i))))))

    ;; Verify task queues exist
    (for ([i num-threads])
         (test-true 
           (queue? 
             (cdr (vector-ref (vector-ref (get-dp-data) 0) i)))))

    ;; Verify task queue semaphores exist
    (for ([i num-threads])
         (test-true 
           (semaphore? 
             (cddr (vector-ref (vector-ref (get-dp-data) 0) i)))))

    ;; Verify hash table of message handlers exists
    (test-true 
      (hash? 
        (car (vector-ref (get-dp-data) 1))))

    ;; Verify hash table semaphore exists
    (test-true 
      (semaphore? 
        (cdr (vector-ref (get-dp-data) 1))))

    ;; Verify parent->dp channel exists
    (test-true 
      (async-channel? 
        (car (vector-ref (get-dp-data) 2))))

    ;; Verify dp->parent channel exists
    (test-true 
      (async-channel? 
        (cdr (vector-ref (get-dp-data) 2))))

    ;; Verify we can kill the datapool environment
    (close-dp *datapool-environment-data*)

    (for ([i num-threads])
         (test-true (thread-dead? 
                      (car 
                        (vector-ref 
                          (vector-ref (get-dp-data) 0) 
                          i))))))
;;**************************************


;;**************************************
;; Get the communication parent->datapool channel
(define (get-dp-channel dp-data) 
  (car (vector-ref dp-data 2)))

;;TEST
(when (defined? *run-3-tests*)
  (let ([num-threads 2]
        [*datapool-environment-data* (make-dp-data num-threads)])

    ;; Check that parent->dp channel exists
    (test-true (async-channel? (get-dp-channel *datapool-environment-data*)))
    (close-dp *datapool-environment-data*)))
;;**************************************



;;;--------------------------------------------------------------------------
;;; Start private datapool thread function defines
;;;-------------------------------------------------------------------------- 
;;; Redefinition of forward declarations
;; Get a thread pid at provided index
(define (get-dp-thread-pid idx)
  (car (vector-ref (vector-ref (get-dp-data) 0) idx)))


;; Get a thread task queue at provided index
(define (get-dp-queue idx)
  (cdr (vector-ref (vector-ref (get-dp-data) 0 ) idx)))


;; Get a thread task queue semaphore at provided index
(define (get-dp-queue-sem idx)
  (cddr (vector-ref (vector-ref (get-dp-data) 0) idx)))


;; Get the vector of message callback handlers
(define (get-dp-message-handlers)
  (car (vector-ref (get-dp-data) 1)))


;; Get the vector of message callback handlers
(define (get-dp-message-handlers-sem)
  (cdr (vector-ref (get-dp-data) 1)))


;; Get the communication channel to the parent scope
(define (get-dp-parent-channel) 
  (cdr (vector-ref (get-dp-data) 2)))


;; Set the global message handlers to something new
(define (set-dp-message-handlers handlers) 
  (vector-set! (get-dp-data) 2 handlers))


;; Get the index of the fullest thread task queue
(define (get-max-dp-q-idx)
  (define longest '((queue-length (get-dp-queue 0)) 0))
  (when (> *num-dp-threads* 1)
    (for ([i (in-range 1 *num-dp-threads*)])
         (let ([cur-q-len (queue-length (get-dp-queue i))])
           (when (> (car longest) cur-q-len)
             (begin
               (set! longest '(cur-q-len i)))))))
  (cdr longest))


;; Get the index of the emptiest thread task queue
(define (get-min-dp-q-idx)
  (define shortest '((queue-length (get-dp-queue 0)) 0))
  (when (> *num-dp-threads* 1)
    (for ([i (in-range 1 *num-dp-threads*)])
         (let ([cur-q-len (queue-length (get-dp-queue i))])
           (when (< (car shortest) cur-q-len)
             (begin
               (set! shortest '(cur-q-len i)))))))
  (cdr shortest))


;; Return thread's queue index if not empty, otherwise gets the index of the 
;; fullest queue.
(define (get-task-q-idx thread-idx)
  (let ([thread-queue (get-dp-queue thread-idx)])
    (if (eqv? (queue-length thread-queue) 0)
        (let ([highest-idx (get-max-dp-q-idx)])
          (if (eqv? (queue-length (get-dp-queue highest-idx)) 0) 
              (thread-suspend (current-thread))
              (highest-idx)))
        (thread-idx))))


;; Return a task from a thread queue to execute
(define (get-task thread-idx)
  (define task '())
  (let ([q-idx (get-task-q-idx thread-idx)])
    (semaphore-wait (get-dp-queue-sem q-idx))
    (set! task (dequeue! (get-dp-queue q-idx)))
    (semaphore-post (get-dp-queue-sem q-idx))
    (task)))


;;Enqueues func expresion to the emptiest thread queue and resumes the thread
(define (go form)
  (let ([q-idx (get-min-dp-q-idx)])
    (semaphore-wait (get-dp-queue-sem q-idx))
    (enqueue! (get-dp-queue q-idx) form)
    (semaphore-post (get-dp-queue-sem q-idx))
    (thread-resume (get-dp-thread-pid q-idx))
    #t))


;; Evaluate given task, guarantees that any one task will not starve the 
;; waiting tasks for cpu time by limiting continuous evaluations
;; Returns: #t if task completed, #f if task not yet completed
(define (dp-thread-eval-task thread-idx task evals-left)
  (let ([ret (eval (task thread-idx))])
    (if (eqv? ret 'alive)
        (if (> evals-left 0)
            (dp-thread-eval-task thread-idx (- evals-left 1))
            (begin 
              (go task) ; place task at the back of a queue
              #f)) ; task not yet completed
        #t))) ; task completed


;; Eternal thread tail recursion of executing tasks
(define (dp-thread thread-idx) 
  ;execute the task we get 
  (dp-thread-eval-task 
    thread-idx 
    (get-task thread-idx) 
    *dp-thread-continuous-eval-limit*)
  (dp-thread thread-idx))


;; Thread startup function
(define (dp-thread-start)
  (let ([pid (current-thread)])
    (thread-suspend pid)
    (define thread-num 0)
    (for ([i *num-dp-threads*])
         (when (eqv? (get-dp-thread-pid i) pid) 
           (set! thread-num i)))
    (dp-thread thread-num)))



;;;--------------------------------------------------------------------------
;;; Create Datapool
;;;--------------------------------------------------------------------------
;; Create a datapool environment. Setup worker threads and begin execution of 
;; the user defined main form. Returns datapool environment data vector needed
;; as the argument for management functions like (close-dp).
(define (datapool num-threads dp-main)
  (let ([*num-dp-threads* num-threads]
        [*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
        [(get-dp-data) (make-dp-data num-threads)])
    (when (> num-threads 0)
      ;Execute the provided dp-main function, kicking off datapool 
      ;activity
      (go '(dp-main)))
    ((get-dp-data))))



;;;----------------------------------------------------------------------------
;;; classes & macros
;;;----------------------------------------------------------------------------
;; message class 
(define message%
  (class object%
         (init 
           [(src inp-src)]
           [(type inp-type)]
           [(args inp-args)])
         (super-new)))


;; Data object interface and class. These will only function properly within 
;; a datapool environment
(define data-interface (interface () 
                                  register-message-handler 
                                  send-message))

(define data%
  (class* object% (data-interface)
          (super-new)

          (define/private (register-message-handler msg-type callback-form)
                          (begin
                            (semaphore-wait (get-dp-message-handlers-sem))
                            (let ([msg-handlers (hash-ref (get-dp-message-handlers)
                                                          msg-type 
                                                          #f)])
                              (if (eqv? #f msg-handlers)
                                  (set-dp-message-handlers 
                                    (hash-set msg-handlers
                                              msg-type 
                                              '(callback-form)))
                                  (set-dp-message-handlers 
                                    (hash-set msg-handlers
                                              msg-type 
                                              (append msg-handlers '(callback-form))))))
                            (semaphore-post (get-dp-message-handlers-sem))
                            #t))

          ;; Send a message to connected handlers in the current datapool
          (define/private (emit msg)
                          (go
                            `(let ([handlers (hash-ref 
                                               (get-dp-message-handlers) 
                                               (msg-type msg))])
                               (when (not (eqv? handlers #f))
                                 (for ([h handlers])
                                      (go 
                                        `(let (msg ,,msg)
                                           h)
                                        ))))))

          ;; Qt-esque connect macro
          (define-syntax connect-message
            (syntax-rules ()
              [(register src-obj msg-type dst-obj handler)
               (register-message-handler 
                 msg-type
                 '(if (eqv? ,src-obj (msg-src msg)) 
                      (send ,dst-obj ,handler (msg-args msg))))]))



          ;; Create task to asynchronously set an object's field. set-field! 
          ;; should be inherently threadsafe (just like normal set!). 
          ;; Ex:
          ;; (go (async my-field '(+ 1 2)))
          (define-syntax async-set!
            (syntax-rules ()
              [(async-set! field val) 
               `(set-field! field ,self val)]
              [(async-set! obj field val)
               `(set-field! field ,obj val)]))))



;;;---------------------------------------------------------------------------- 
;;; Feature Test
;;;---------------------------------------------------------------------------- 
;TODO figure out how to get argv & argc 
;(define dp1 (datapool 4 '(main argv argc)))
;(let ([ch (get-dp-channel dp1)]) 
;(<- ch))
