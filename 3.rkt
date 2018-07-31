#lang racket
(require racket/base
         racket/class
         racket/async-channel
         data/queue
         delay-pure)


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



;;;----------------------------------------------------------------------------
;;;datapool functions
;;;----------------------------------------------------------------------------
;;create datapool data 
(define (make-dp-data num-threads) 
  (vector 
    ;thread id's, queues, and semaphores
    (make-vector num-threads '((thread (thunk (dp-thread-start)))
                               (make-queue) 
                               (make-semaphore 1))) 
    (make-hash) ;hash table of lists of message handlers
    '((channel #f) (channel #f)) ;channel to datapool parent scope

;; Get the communication channel to the datapool scope
(define (get-dp-channel data) 
  (car (vector-ref data 2)))

;;create a datapool
;;setup worker threads and begin execution of the user defined func
(define (datapool num-threads dp-main)
  ;;;--------------------------------------------------------------------------
  ;;; Private datapool thread functions
  ;;;--------------------------------------------------------------------------
  ;; Get a thread pid at provided index
  (define (get-dp-thread-id idx)
    (car (vector-ref (vector-ref *datapool-environment-data* 0) idx)))


  ;; Get a thread task queue at provided index
  (define (get-dp-queue idx)
    (cdr (vector-ref (vector-ref *datapool-environment-data* 0 ) idx)))


  ;; Get a thread task queue semaphore at provided index
  (define (get-dp-queue-sem idx)
    (cddr (vector-ref (vector-ref *datapool-environment-data* 0) idx)))


  ;; Get the vector of message callback handlers
  (define (get-dp-message-handlers)
    (vector-ref *datapool-environment-data* 1))


  ;; Get the communication channel to the parent scope
  (define (get-dp-parent-channel) 
    (cdr (vector-ref *datapool-environment-data* 2)))


  ;; Set the global message handlers to something new
  (define (set-dp-message-handlers handlers) 
    (vector-set! *datapool-environment-data* 2 handlers))


  ;; Get the index of the fullest thread task queue
  (define (get-max-dp-q-idx)
    (define longest '((queue-length (get-dp-queue 0)) 0))
    (if (> *num-dp-threads* 1)
        (for ([i (in-range 1 *num-dp-threads*)])
             (let ([cur-q-len (queue-length (get-dp-queue i))])
               (if (> (car longest) cur-q-len))
               (begin
                 (set! longest '(cur-q-len i))))))
    (cdr longest))


  ;; Get the index of the emptiest thread task queue
  (define (get-min-dp-q-idx)
    (define shortest '((queue-length (get-dp-queue 0)) 0))
    (if (> *num-dp-threads* 1)
        (for ([i (in-range 1 *num-dp-threads*)])
             (let ([cur-q-len (queue-length (get-dp-queue i))])
               (if (< (car shortest) cur-q-len))
               (begin
                 (set! shortest '(cur-q-len i))))))
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
      (thread-resume (get-dp-thread-id q-idx))))


  ;; Eternal thread tail recursion of executing tasks
  (define (dp-thread thread-idx) 
    ;execute the task we get
    (eval (get-task thread-idx))  
    (dp-thread thread-idx))


  ;; Thread startup function
  (define (dp-thread-start)
    (thread-suspend id)
    (let ([id (current-thread)])
      (define thread-num 0)
      (for ([i *num-dp-threads*])
           (if 
             (eqv? (vector-ref *datapool-environment-data* i) id) 
             (set! thread-num i)))
      (dp-thread i)))

  (let ([*num-dp-threads* num-threads]
        [*datapool-environment-data* (make-dp-data num-threads)]
        (if (> num-threads 0)
            (begin 
              ;The following define is *only* used for syncing message handlers
              (define *dp-data-message-handler-sem* (make-semaphore 1))

              ;Execute the provided dp-main function, kicking off datapool 
              ;activity
              (go '(dp-main))))
        (*datapool-environment-data*))))


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
;;; classes & macros
;;;----------------------------------------------------------------------------
;; Data object interface and class. These will only function properly within 
;; a datapool environment
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
                          `(let ([msg ,inp-msg] 
                                 [handlers (hash-ref (get-dp-message-handlers) 
                                                     (msg-type msg))])
                             (if (not (eqv? handlers #f))
                                 (for ([h handlers])
                                      (go 
                                        `(let (msg ,,inp-msg)
                                           h)
                                        )))))
                        #t))


;; message class
(class message%
       (super-new)
       (init 
         [(src inp-src)]
         [(type inp-type)]
         [(args inp-args)])
       (define/public src)
       (define/public type)
       (define/public args))


(define-syntax connect-message
  (syntax-rules ()
    [(register src-obj msg-type dst-obj handler)
     (register-message-handler 
       msg-type
       '(if (eqv? ,src-obj (msg-src msg)) 
            (send ,dst-obj ,handler (msg-args msg))))]))


;; Send a message to connected handlers in the current datapool
(define-syntax send-message
  (syntax-rules ()
    [(send msg) 
     (send-message self msg)]))


;; define a pure stateless function. These should always be safe when executed 
;; asynchronously with a (go) call
(define-syntax pure-func
  (syntax-rules ()
    [(definep ...) 
     (define-pure/stateless ...)]))


;; Asynchronously set an object's field. set-field! should be inherently 
;; threadsafe (just like normal set!). 
;; Ex:
;; (bind my-field '(+ 1 2))
(define-syntax async-set-field
  (syntax-rules ()
    [(bind field val)
     (go `(set-field! field ,self val))]))



;;;----------------------------------------------------------------------------
;;; Test stuff
;;;----------------------------------------------------------------------------
;TODO figure out how to get argv & argc 
(define dp1 (datapool 4 '(main argv argc)))
(let ([ch (get-dp-parent-channel dp1)]) 
  (<- ch))
