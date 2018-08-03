#lang racket
(require racket/base
         racket/class
         racket/async-channel
         data/queue
         delay-pure) 

;;;----------------------------------------------------------------------------
;;; Test Functions 
;;;---------------------------------------------------------------------------- 
;; To run unit tests (define *run-3-tests* #t) before loading. To wait for user 
;; input on test failure (define *run-3-tests-wait-before-cont* #t)

;; Return #t if the given identifier is defined, else #f
(define-syntax (defined? stx)
  (syntax-case stx ()
               [(_ id)
                (with-syntax ([v (identifier-binding #'id)])
                             #''v)])) 


;; Return #t if the quoted form returns #t, else #f 
(define (test-true? form)
  (if (eqv? form #t)
      (begin
        (printf "pass: ~a == #t" form)
        #t)
      (begin
        (printf "FAIL: ~a == #f" form)
        (when (defined? *run-3-tests-wait-before-cont*)
          (read-line (current-input-port) 'any))
        #f)))


;; Return #t if quoted forms return an equal value, else #f
(define (test-equal? form-a form-b)
  (if (eqv? form-a form-b)
      (begin
        (printf "pass: ~a == ~a" form-a form-b)
        #t)
      (begin
        (printf "FAIL: ~a == ~a" form-a form-b)
        (when (defined? *run-3-tests-wait-before-cont*)
          (read-line (current-input-port) 'any))
        #f))) 


;; Custom test fail
(define (test-fail form)
  (printf "FAIL: ~a" form)
  (when (defined? *run-3-tests-wait-before-cont*)
    (read-line (current-input-port) 'any)))


;; Custom test pass
(define (test-pass form)
  (printf "pass: ~a" form))



;;;----------------------------------------------------------------------------
;;; Pure/Stateless Coroutines
;;;----------------------------------------------------------------------------  
;; Forward define for yield 
(define (yield val) val)


;; Coroutine definition  
(define (make-generator procedure)
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
                              (begin
                                (raise 
                                  (make-exn:fail:user
                                    '("Function is not pure: " 
                                      (define (name . params) body ...))))
                                '()))))


;; DO *NOT* USE IF YOU DON'T HAVE TO FOR PERFORMANCE REASONS
;; Define a coroutine with no purity/state checks. (func) can be slow because 
;; it fully expands the form checking for purity/statelessness. This rule skips
;; that check, which makes it *dangerous* when used in the multithreaded (go)
;; environment. Therefore, even if you use this function you should still 
;; validate its purity, manually if necessary.
(define-syntax-rule (danger-func! (name . params) body ...)
                    (define-coroutine (name . params)
                                      (begin
                                        (yield 'alive) 
                                        body
                                        ...
                                        #f)))



;;;----------------------------------------------------------------------------
;;;basic channel functions
;;;----------------------------------------------------------------------------
;;create an async channel, no size limit by default
(define (channel [size #f]) (make-async-channel size))


;;channel get, blocks by default
(define (<- ch [block #t])
  (if block
      (async-channel-get ch)
      (async-channel-try-get ch)))


;;channel put
(define (-> ch item)
  (async-channel-put ch item))



;;;--------------------------------------------------------------------------
;;; Globals and global forward declarations
;;;--------------------------------------------------------------------------
;; Number of threads 
(define *num-dp-threads* #f)

;; Current datapool environment data
(define *datapool-environment-data* #f)

;; Number of times a coroutine is to be evaluated in a row before swapping tasks
(define *dp-thread-continuous-eval-limit* 10) 

;; Forward definitions
(define (dp-thread-start) (thread-suspend (current-thread)))
(define (go form) #f)



;;;--------------------------------------------------------------------------
;;; Public Datapool Functions
;;;--------------------------------------------------------------------------
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


;; Get the communication parent->datapool channel
(define (get-dp-channel dp-data) 
  (car (vector-ref dp-data 2)))



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



;;;----------------------------------------------------------------------------
;;; Create Datapool
;;;---------------------------------------------------------------------------- 
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

          (define/private 
            (register-message-handler msg-type callback-form)
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
          (define/private 
            (emit msg)
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
;;; Unit Tests
;;;---------------------------------------------------------------------------- 
(when (defined? *run-3-tests*) (load "3-ut.rkt"))
