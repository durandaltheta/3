#lang racket
(require racket/base
         racket/class
         racket/async-channel
         data/queue
         delay-pure) 

(provide datapool ;start a datapool with given env data and form to execute
         make-datapool-data ;make datapool environment data
         close-dp ;kill all threads in the datapool
         channel ;create an asynchronous channel
         <- ;get form from a channel
         -> ;send form into a channel
         get-datapool-channel ;get the channel to send messages into the dp
         send-to-datapool-parent ;send data to the datapool's parent scope
         data ;create data object and register it in the datapool 
         get-dp-data-object ;get an object via its key
         message% ;create a message object with a given type and arguments
         go ;place a coroutine in a queue to be executed by a thread
         definep ;define a pure & stateless function
         func ;define a pure & stateless coroutine; use in (go)
         danger-func! ;define a coroutine with no state checks
         test-true?
         test-equal?
         test-fail
         test-pass
         run-unit-tests)

;;;----------------------------------------------------------------------------
;;; Test Functions 
;;;---------------------------------------------------------------------------- 
;; To wait for user input on test failure 
;; (define *run-3-tests-wait-before-cont* #t) 

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



;; Define a namespace so eval can run correctly
(define-namespace-anchor eval-anchor)
(define eval-namespace (namespace-anchor->namespace eval-anchor))


;; Define a pure & stateless function
(define-syntax-rule (definep (name . params) body ...)
                    (define-pure/stateless (name . params) body ...))


;; Define a coroutine of a pure stateless coroutine. These should always be 
;; safe when executed asynchronously with a (go) call. The current 'coroutine' 
;; portion of the definition *is* stateful, only the provided form is 
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
                                '())) eval-namespace))


;; DO *NOT* USE UNLESS REQUIRED FOR PERFORMANCE REASONS
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
;; Current datapool environment data
(define *datapool-environment-data* #f)

;; Number of times a coroutine is to be evaluated in a row before swapping tasks
(define *dp-thread-continuous-eval-limit* 10) 

;; global counter for data object hash keys
(define *data-obj-key-src* 0)
(define *data-obj-free-key-q* #f)

;;;--------------------------------------------------------------------------
;;; Public Datapool Functions
;;;--------------------------------------------------------------------------
;;create datapool data 
(define (make-datapool-data num-threads) 
  (box ;return a 'by reference' symbol
    (vector 
      ;thread id's, queues, and semaphores
      (make-vector 
        num-threads 
        (make-vector 
          (thread (thunk (dp-thread-start))) ;threads
          (make-queue) ;thread task queues
          (make-semaphore 1))) ;thread task queue semaphores
      (make-vector 
        (make-hash) ;hash table of data objects
        (make-hash) ;hash table of lists of message handlers
        (make-semaphore 1) ;data hash semaphore
        (make-semaphore 1)) ;message handlers semaphore
      (make-vector 
        (channel #f) ;parent->datapool channel
        (channel #f) ;datapool->parent channel
        (make-semaphore))))) ;datapool->parent channel semaphore


;; Return the datapool's data
(define (get-dp-data)
  (unbox *datapool-environment-data*))


;; Return number of threads in the datapool
(define (get-num-dp-threads) 
  (vector-length (vector-ref (get-dp-data) 0)))


;;kill all threads in a datapool
(define (close-dp dp-data)
  (let ([data (unbox dp-data)])
    (for ([i (vector-length (vector-ref data 0))])
         (kill-thread (car (vector-ref (vector-ref data 0) i))))))


;; Get the communication parent->datapool channel
(define (get-datapool-channel dp-data) 
  (let ([data (unbox dp-data)])
    (vector-ref (vector-ref data 2) 0))) 



;;;--------------------------------------------------------------------------
;;; Start private datapool thread function defines
;;;-------------------------------------------------------------------------- 
;; Get a thread pid at provided index
(define (get-dp-thread idx)
  (vector-ref (vector-ref (vector-ref (get-dp-data) 0) idx) 0))


;; Get a thread task queue at provided index
(define (get-dp-queue idx)
  (vector-ref (vector-ref (vector-ref (get-dp-data) 0 ) idx) 1))


;; Get a thread task queue semaphore at provided index
(define (get-dp-queue-sem idx)
  (vector-ref (vector-ref (vector-ref (get-dp-data) 0) idx) 2))


;; Get the hash of data objects
(define (get-dp-data-objects)
  (vector-ref (vector-ref (get-dp-data) 1) 0))


;; Get the data objects semaphore
(define (get-dp-data-objects-sem)
  (vector-ref (vector-ref (get-dp-data) 1) 2)) 


;; Add a recycled data object key to the container queue
(define (add-free-dp-data-key key) 
  (when (queue? *data-obj-free-key-q* #f)
    (enqueue! *data-obj-free-key-q* key)))


;; Generate a new data object hash key
(define (gen-dp-data-obj-key) 
  (let ([ret *data-obj-key-src*])
    (if (> 
          (if (queue? *data-obj-free-key-q* #f)
              (queue-length *data-obj-free-key-q*)
              0) 
          0)
        (dequeue! *data-obj-free-key-q*)
        (let ()
          (set! *data-obj-key-src* (+ *data-obj-key-src* 1))
          ret))))


;; Get the index of the fullest thread task queue
(define (get-max-dp-q-idx)
  (define longest '((queue-length (get-dp-queue 0)) 0))
  (when (> (get-num-dp-threads) 1)
    (for ([i (in-range 1 (get-num-dp-threads))])
         (let ([cur-q-len (queue-length (get-dp-queue i))])
           (when (> (car longest) cur-q-len)
             (begin
               (set! longest '(cur-q-len i)))))))
  (cdr longest))


;; Get the index of the emptiest thread task queue
(define (get-min-dp-q-idx)
  (define shortest '((queue-length (get-dp-queue 0)) 0))
  (when (> (get-num-dp-threads) 1)
    (for ([i (in-range 1 (get-num-dp-threads))])
         (let ([cur-q-len (queue-length (get-dp-queue i))])
           (when (< (car shortest) cur-q-len)
             (begin
               (set! shortest '(cur-q-len i)))))))
  (cdr shortest))


;;Enqueues func expresion to the emptiest thread queue and resumes the thread
(define (go form)
  (let ([q-idx (get-min-dp-q-idx)])
    (semaphore-wait (get-dp-queue-sem q-idx))
    (enqueue! (get-dp-queue q-idx) form)
    (semaphore-post (get-dp-queue-sem q-idx))
    (thread-resume (get-dp-thread q-idx))
    #t))


;; Hash new data object
(define (hash-dp-data-object key obj)
  (if (not (get-dp-data-object key) #f)
      #f ;we've run out of possible hash table values and looped?
      (let ()
        (semaphore-wait (get-dp-data-objects-sem))
        (hash (get-dp-data-objects) key obj)
        (semaphore-post (get-dp-data-objects-sem))
        (define-coroutine 
          (run-handler)
          (send (get-dp-data-object key) run))
        (go run-handler)
        #t)))


;; Get a data object from the hash
(define (get-dp-data-object key)
  (hash-ref (get-dp-data-objects) key #f))


;; Delete and remove a data object from the hash
(define (delete-dp-data-object key)
  (semaphore-wait (get-dp-data-objects-sem))
  (hash-set! (get-dp-data-objects) key #f)
  (semaphore-post (get-dp-data-objects-sem)))


;; Get the hash of message callback handlers
(define (get-dp-message-handlers)
  (vector-ref (vector-ref (get-dp-data) 1) 1))


;; Get the message callback handlers semaphore
(define (get-dp-message-handlers-sem)
  (vector-ref (vector-ref (get-dp-data) 1) 3))


;; Set the global message handlers to something new
(define (set-dp-message-handlers handlers) 
  (semaphore-wait (get-dp-message-handlers-sem))
  (vector-set! (vector-ref (get-dp-data) 1) 3 handlers)
  (semaphore-post (get-dp-message-handlers-sem)))


(define (get-dp-parent-channel/internal) 
  (vector-ref (vector-ref (get-dp-data) 2) 1))


;; Get the semaphore for the parent->dp channel
(define (get-dp-parent-ch-sem)
  (vector-ref (vector-ref (get-dp-data) 2) 2))


;; Send info from the datapool to the parent
(define (send-to-datapool-parent form)
  (semaphore-wait (get-dp-parent-ch-sem))
  (-> (get-dp-parent-channel/internal))
  (semaphore-post (get-dp-parent-ch-sem)))


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


;; Evaluate given task, guarantees that any one task will not starve the 
;; waiting tasks for cpu time by limiting continuous evaluations
;; Returns: #t if task completed, #f if task not yet completed
(define (dp-thread-eval-task thread-idx task evals-left)
  (let ([ret (task)])
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
    (for ([i (get-num-dp-threads)])
         (when (eqv? (get-dp-thread i) pid) 
           (set! thread-num i)))
    (dp-thread thread-num)))



;;;----------------------------------------------------------------------------
;;; Create Datapool
;;;---------------------------------------------------------------------------- 
;; Create a datapool environment. Setup worker threads and begin execution of 
;; the user defined main form. Returns datapool environment data vector needed
;; as the argument for management functions like (close-dp).
(define (datapool datapool-data dp-main)
  (let ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
        [*datapool-environment-data* datapool-data]
        [*data-obj-key-src* 0]
        [*data-obj-free-key-q* (make-queue)])
    (if (> (get-num-dp-threads) 0)
        (let ()
          (go dp-main) ;Execute the provided dp-main function
          (get-dp-data))
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
                                  register-message-handler 
                                  send-message))

;; Data object class
(define data%
  (class* 
    object% 
    (data-interface)
    (init-field 
      key
      [deleted #f])
    (super-new)

    ;;,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    ;; Private Methods and Fields
    ;;,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    ;; Register a message callback
    (define/private 
      (register-message-handler msg-type callback-form)
      (if (not (deleted?))
          (let ()
            (semaphore-wait (get-dp-message-handlers-sem))
            (let ([msg-handlers (hash-ref (get-dp-message-handlers)
                                          msg-type 
                                          #f)])
              (if (eqv? #f msg-handlers)
                  (set-dp-message-handlers 
                    (hash-set msg-handlers
                              msg-type 
                              callback-form))
                  (set-dp-message-handlers 
                    (hash-set msg-handlers
                              msg-type 
                              (append msg-handlers '(key callback-form))))))
            (semaphore-post (get-dp-message-handlers-sem))
            #t)
          'deleted))

    ;; Send a message to connected handlers in the current datapool
    (define/private 
      (send msg)
      (if (not (deleted?))
          (let ([handlers (hash-ref 
                            (get-dp-message-handlers) 
                            (get-field type msg))])
            (when (not (eqv? handlers #f))
              (for 
                ([hpair handlers])
                (go (lambda ()
                      (let ([msg msg]
                            [h (cdr hpair)])
                        (h)))))))
          'deleted))

    ;;,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    ;; Public Methods and Fields
    ;;,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    ;; Set the current data object to the deleted state (won't accept any more
    ;;
    (define/public (delete)
                   (set! deleted #t))

    ;; Check whether object is still valid
    (define/private
      (deleted?)
      deleted)

    ;; Create a message object 
    (define/public (message inp-type inp-args)
                   (make-object message% key inp-type inp-args))

    ;; Qt-esque connect macro
    (define-syntax receive
      (syntax-rules ()
        [(receive src-obj-key msg-type dst-obj-key handler)
         (if (not (deleted?))
             (register-message-handler 
               msg-type
               (let ()
                 (define-coroutine
                   (handler-accessor)
                   (if (eqv? (get-dp-data-object dst-obj-key) #f)
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
    ;; (func (gimme-3) (+ 1 2))
    ;; (go (set-datum! my-field gimme-3))
    (define-syntax set-datum!
      (syntax-rules ()
        [(set-datum! field val) 
         (if (not (deleted?))
             (let ()
               (define-coroutine
                 (set-handler)
                 (let ([obj (get-dp-data-object key)])
                   (if (eqv? obj #f)
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
                           "set-field failed with obj-key:~a; field: ~a; val: 
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
                   (if (eqv? obj #f)
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
                           "set-field failed with obj-key:~a; field: ~a; val: 
                           ~a" 
                           key 
                           field 
                           val)))))
               (go set-handler))
             'deleted)]))))


;; Create and register a new data object with the datapool. Do not (emit), 
;; (connect), (set-datum!), or (go) in the object arguments or you may have a 
;; bad time.
(define-syntax-rule 
  (data (name% . params) body ...)
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
                           (make-object name% key . params))))) 


;; Data object destructor. Sets object to the 'deleted state, removes object 
;; (receive) callbacks, removes the object from the object hash, and enqueues 
;; the now free object key onto a free floating key queue
(define (delete-data key) 
  (let ([obj (get-dp-data-object key)])
    (when (not (eqv? obj #f))
      (let ()
        ;Inform the data object it is deleted. This should stop it from 
        ;executing further (receive), (send msg), 
        (send obj delete)

        (define (remove-obj-callbacks hkey value) 
          ;Each h is a list of pairs in the form '(dst-obj-key dst-obj-callback)
          (for ([h (length value)])
               ;Return a list without handler pairs whose dst-obj-key 
               ;matches key
               (filter (not (eqv? (car h) key)))))

        ;Remove all callbacks to the data object mapped to key
        (semaphore-wait (get-dp-message-handlers-sem))
        (hash-for-each 
          (get-dp-message-handlers) 
          remove-obj-callbacks)
        (semaphore-post (get-dp-message-handlers-sem))

        ;Remove the data object from the hash
        (delete-dp-data-object key)
        (add-free-dp-data-key key))))) 



;;;---------------------------------------------------------------------------- 
;; Run unit tests
;;;---------------------------------------------------------------------------- 
(define (run-unit-tests)
  (load "3-ut.rkt"))
