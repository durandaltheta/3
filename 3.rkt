#lang racket
(require racket/base
         racket/class
         racket/async-channel
         data/queue
         pure/stateless)


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
  (async-channel-put ch))



;;;----------------------------------------------------------------------------
;;;couroutine definition 
;;;----------------------------------------------------------------------------
;; Coroutines can 'pause' execution at arbitrary points by (yield) ing 

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


(define (coroutine form)
  (make-generator
    (lambda (yield)
      (yield #t)
      (eval form)
      (yield #f))))



;;;----------------------------------------------------------------------------
;;;datapool functions
;;;----------------------------------------------------------------------------
;;create a datapool
;;setup worker threads and begin execution of the user defined func
(define (datapool num-threads dp-main args)
  ;;create datapool data 
  (define (make-dp-data num-threads) 
    (vector 
      ;thread id's, queues, and semaphores
      (make-vector num-threads '((thread '(dp-thread-start num-threads))
                                 (make-queue) 
                                 (make-semaphore 1))) 
      (make-hash) ;hash table of lists of message handlers
      (channel #f))) ;channel to datapool parent scope

  (let ([*num-dp-threads* num-threads]
        [*dp* (make-dp-data num-threads)]
        (if (> num-threads 0)
          (begin 
            ;The following define is *only* used for syncing message handlers
            (define *dp-data-message-handler-sem* (make-semaphore 1))

            ;The following is the load balancing rule. When it returns #f, the
            ;given thread pauses execution of its current coroutine, places
            ;the coroutine at the end of its task queue, and grabs the next task
            (define *dp-thread-load-rule* (base-load-rule 10))

            ;Execute the provided dp-main function, kicking off datapool 
            ;activity
            (go *dp* (dp-main args))))
        (*dp*))))


;;kill all threads in a datapool
(define (close-dp dp-data)
  (for ([i (vector-length (vector-ref dp-data 0))])
       (kill-thread (car (vector-ref (vector-ref dp-data 0) i)))))


;;return a message from the datapool's channel
(define (listen-dp dp-data block)
  (let ([ch (vector-ref dp-data 2)])
    (<- ch block))) 


;;send a message to the datapool
(define (send-dp dp-data msg)
  (let ([ch (vector-ref dp-data 2)])
    (-> ch msg)))



;;;----------------------------------------------------------------------------
;;;datapool thread functions
;;;----------------------------------------------------------------------------
(define (get-dp-thread-id idx)
  (car (vector-ref (vector-ref *dp* 0) idx)))


(define (get-dp-queue idx)
  (cdr (vector-ref (vector-ref *dp* 0 ) idx)))


(define (get-dp-queue-sem idx)
  (cddr (vector-ref (vector-ref *dp* 0) idx)))


(define (get-dp-message-handlers)
  (vector-ref *dp* 1))


(define (get-dp-channel dp-data) 
  (vector-ref *dp* 2))


(define (set-dp-message-handlers handlers) 
  (vector-set! *dp* 2 handlers))


(define (get-max-dp-q-idx)
  (define longest '((queue-length (get-dp-queue 0)) 0))
  (if (> *num-dp-threads* 1)
    (for ([i (in-range 1 *num-dp-threads*)])
         (let ([cur-q-len (queue-length (get-dp-queue i))])
           (if (> (car longest) cur-q-len))
           (begin
             (set! longest '(cur-q-len i))))))
  (cdr longest))


(define (get-min-dp-q-idx)
  (define shortest '((queue-length (get-dp-queue 0)) 0))
  (if (> *num-dp-threads* 1)
    (for ([i (in-range 1 *num-dp-threads*)])
         (let ([cur-q-len (queue-length (get-dp-queue i))])
           (if (< (car shortest) cur-q-len))
           (begin
             (set! shortest '(cur-q-len i))))))
  (cdr shortest))


;; return thread's queue index if not empty, otherwise gets the index of the 
;; fullest queue.
(define (get-task-q-idx thread-idx)
  (let ([thread-queue (get-dp-queue thread-idx)])
    (if (eqv? (queue-length thread-queue) 0)
      (let ([highest-idx (get-max-dp-q-idx)])
        (if (eqv? (queue-length (get-dp-queue highest-idx)) 0) 
          (thread-suspend (current-thread))
          (highest-idx)))
      (thread-idx))))


(define (get-task thread-idx)
  (define task '(coroutine))
  (let ([q-idx (get-task-q-idx thread-idx)])
    (semaphore-wait (get-dp-queue-sem q-idx))
    (set! task (dequeue! (get-dp-queue q-idx)))
    (semaphore-post (get-dp-queue-sem q-idx))
    (task)))


;; Simplest possible load rule, returns itself with num-1. When num hits 0, 
;; returns #f. This means any given coroutine task will only evaluate num times
;; in a row before being rotated to the end of the queue
(define (base-load-rule num) 
  (if (<= num 0) `(base-load-rule ,(- num 1)) #f))


;; Evaluate a coroutine. This will either be a standard function call OR 
;; message handling. If the evaluated task yields and is still active will 
;; continue evaluation until load-rule returns #f
(define (eval-task task load-rule)
  (let ([ret (eval task)])
    (if (ret #t) 
      (let ([rule (eval load-rule)])
        (if (rule) 
          (eval-task task rule)
          (go task))))))


;;Enqueues func expresion to the emptiest thread queue and resumes the thread
(define (go form)
  (let ([q-idx (get-min-dp-q-idx)])
    (semaphore-wait (get-dp-queue-sem q-idx))
    (enqueue! (get-dp-queue q-idx) form)
    (semaphore-post (get-dp-queue-sem q-idx))
    (thread-resume (get-dp-thread-id q-idx))))


;;eternal thread tail recursion of executing tasks
(define (dp-thread thread-idx) 
  ;execute the task we get
  (eval-task (get-task thread-idx) *dp-thread-load-rule*)  
  (dp-thread thread-idx))


;;thread startup function
(define (dp-thread-start)
  (thread-suspend id)
  (let ([id (current-thread)])
    (define thread-num 0)
    (for ([i *num-dp-threads*])
         (if 
           (eqv? (vector-ref *dp* i) id) 
           (set! thread-num i)))
    (dp-thread i)))



;;;----------------------------------------------------------------------------
;;; classes & macros
;;;----------------------------------------------------------------------------
;; data object interface and class
(define data-interface (interface () 
                                  register-message-handler 
                                  send-message))

(class* data% (data-interface)
        (super-new)
        (define/private (register-message-handler msg-type callback-form)
                        (begin
                          (semaphore-wait *dp-data-message-handler-sem*)
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
                          (semaphore-post kdp-data-message-handler-sem*)
                          #t))
        (define/private (send-message inp-msg)
                        (go
                          `(couroutine `(let ([msg ,,inp-msg] 
                                              [handlers (hash-ref (get-dp-message-handlers) 
                                                                  (msg-type msg))])
                                          (if (not (eqv? handlers #f))
                                            (for ([h handlers])
                                                 (go h))))))
                        #t))

;; message class
(class message%
       (init 
         [(src inp-src)]
         [(type inp-type)]
         [(args inp-args)])
       (define/private src)
       (define/private type)
       (define/private args)
       (define/public (msg-src)(src))
       (define/public (msg-type)(type))
       (define/public (msg-args)(args)))

(define-syntax connect-message
  (syntax-rules ()
                [(connect src-obj msg-type dst-obj handler)
                 (register-message-handler 
                   msg-type
                   `(couroutine '(if (eqv? ,src-obj (msg-src msg)) 
                                   (send ,dst-obj ,handler (msg-args msg)))))]))

(define-syntax send-message
  (syntax-rules ()
                [(emit msg) 
                 (send-message self msg)]))

;; define a stateless couroutine. These should always be safe when executed 
;; asynchronously with a (go) call
(define-syntax pure-func
  (syntax-rules ()
                [(func ...) (couroutine 
                              '(begin
                                 (yield #t)
                                 (pure/stateless (define ...))
                                 (yield #f)))]))

;; Asynchronously set a public field. set-field! should be inherently 
;; threadsafe (just like normal set!). 
;; Ex:
;; (: my-field '(+ 1 2))
(define-syntax async-set-field
  (syntax-rules ()
                [(: field val)
                 (go `(set-field! field ,self val))]))

;;;----------------------------------------------------------------------------
;;; Optional 3 definitions, if channels are insufficient
;;;----------------------------------------------------------------------------
;;define a message
(define (message) ()) 

;;emit message
(define (say source message) ()) 

;;listen for message
(define (listen source message destination function) ()) 



;;execute 3
;TODO figure out how to get argv & argc 
(define (get-num-cpu) (4)) 
(define dp1 (datapool (- (get-num-cpus) 1) main '(argv argc)))
(let ([ch (get-dp-channel dp1)]) 
  (<- ch))
