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
         get-datapool-channel ;get the channel to send messages into the dp
         send-to-datapool-parent ;send data to the datapool's parent scope
         data ;create data object and register it in the datapool 
         get-dp-data-object ;get an object via its key
         message% ;create a message object with a given type and arguments
         go ;place a coroutine in a queue to be executed by a thread
         definep ;define a pure & stateless coroutinetion
         coroutine ;return a coroutine thunk for use in (go)
         ;danger-coroutine! ;define a coroutine with no state checks
         test-true?
         test-equal?
         test-fail
         test-pass
         run-unit-tests)

;;;----------------------------------------------------------------------------
;;; Test coroutinetions 
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
(define (test-true? description form [wait #f])
  (if (eqv? form #t)
      (let ()
        (printf "pass ~a: #t\n\n" description)
        #t)
      (let ()
        (printf "FAIL ~a: ~a != #t\n" description form)
        (if wait
            (let ()
              (printf "<enter to continue>")
              (read-line (current-input-port) 'any)
              (printf "\n"))
            (printf "\n"))
        #f)))


;; Return #t if quoted forms return an equal value, else #f
(define (test-equal? description form-a form-b [wait #f])
  (if (eqv? form-a form-b)
      (let ()
        (printf "pass ~a: ~a == ~a\n\n" description form-a form-b)
        #t)
      (let ()
        (printf "FAIL ~a: ~a != ~a\n" description form-a form-b)
        (if wait
            (let ()
              (printf "<enter to continue>")
              (read-line (current-input-port) 'any)
              (printf "\n"))
            (printf "\n"))
        #f))) 


;; Custom test fail
(define (test-fail description form [wait #f])
  (printf "FAIL ~a: ~a\n\n" description form)
  (when wait
    (read-line (current-input-port) 'any)))


;; Custom test pass
(define (test-pass description form)
  (printf "pass ~a: ~a\n\n" description form))



;;;----------------------------------------------------------------------------
;;; Pure/Stateless Coroutines
;;;----------------------------------------------------------------------------  
;; Forward define for yield 
(define (yield val) val)

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
                     ((eq? (car args) 'status?) status)
                     ((eq? (car args) 'dead?) (eq? status 'dead))
                     ((eq? (car args) 'alive?) (not (eq? status 'dead)))
                     ((eq? (car args) 'kill!) (set! status 'dead))
                     (#t (apply last-continuation args)))))))


(define-syntax (coroutine stx)
  (syntax-case stx ()
               ((_ (name . args) . body )
                #`(define (name . args)
                    (make-generator
                      (lambda (#,(datum->syntax stx 'yield))
                        . body))))))


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
(define-syntax-rule (coroutine (name . params) body ...)
                    (eval (let ()
                            ;Special check to see if given coroutinetion symbols 
                            ;produce a pure & stateless coroutinetion. Otherwise
                            ;should raie an error!
                            (let ([def (definep (name . params)
                                                body
                                                ...)])
                              (void))
                            '(coroutine (name . params)
                                        (begin
                                          (yield 'alive) 
                                          body
                                          ...))) 
                          eval-namespace))


;; DO *NOT* USE UNLESS REQUIRED FOR PERFORMANCE REASONS
;; Define a coroutine with no purity/state checks. (coroutine) can be slow because 
;; it fully expands the form checking for purity/statelessness. This rule skips
;; that check, which makes it *dangerous* when used in the multithreaded (go)
;; environment. Therefore, even if you use this coroutinetion you should still 
;; validate its purity, manually if necessary.
(define-syntax-rule (danger-coroutine! (name . params) body ...)
                    (coroutine (name . params)
                               (begin
                                 (yield 'alive) 
                                 body
                                 ...)))
|#


;;;----------------------------------------------------------------------------
;;;basic channel coroutinetions
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
;;; Globals and global forward declarations
;;;--------------------------------------------------------------------------
;; Number of times a coroutine is to be evaluated in a row before swapping tasks
(define *dp-thread-continuous-eval-limit* 10) 



;;;--------------------------------------------------------------------------
;;; Datapool Data coroutinetions
;;;--------------------------------------------------------------------------
;;create datapool data 
(define (make-dp-data num-threads eval-limit) 
  (let ([ret (box #f)]
        [key-src 0])
    (set-box! ret 
              (vector 
                ;thread id's, queues, and semaphores
                (make-vector ;thread and task queue vector
                  num-threads 
                  (vector 
                    (thread (thunk (dp-thread-start ret))) ;threads
                    (make-queue) ;thread task queues
                    (make-semaphore 1))) ;thread task queue semaphores 
                (vector ;message handler vector
                  (make-hash) ;hash table of data objects
                  (make-hash) ;hash table of lists of message handlers
                  (make-semaphore 1) ;data hash semaphore
                  (make-semaphore 1)) ;message handlers semaphore
                (vector ;datapool channel vector
                  (channel #f) ;parent->tdatapool channel
                  (channel #f) ;datapool->parent channel
                  (make-semaphore)) ;datapool->parent channel semaphore 
                (vector ;data object vector
                  key-src ;data-obj-key-src
                  (make-queue)) ;data-obj-free-key-q 
                (vector ;evaluation rules vector
                  eval-limit)))
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
       (kill-thread (car (vector-ref (vector-ref (get-dp-data env) 0) i)))))


;; Get the communication parentch-putdatapool channel
(define (get-datapool-channel env) 
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


;; Get the queue of freed data keys
(define (get-data-obj-free-key-q env)
  (vector-ref (vector-ref (get-dp-data env) 3) 1))


(define (get-dp-thread-continuous-eval-limit env)
  (vector-ref (vector-ref (get-dp-data env) 4) 0))


;; Add a recycled data object key to the container queue
(define (add-free-dp-data-key env key) 
  (when (queue? (get-data-obj-free-key-q env) #f)
    (enqueue! (get-data-obj-free-key-q env) key)))


;; Generate a new data object hash key
(define (gen-dp-data-obj-key env) 
  (if (> (queue-length (get-data-obj-free-key-q env)) 0) 
      (dequeue! (get-data-obj-free-key-q env))
      (let ([ret (get-data-obj-key-src env)])
        (set! (get-data-obj-key-src env) (+ (get-data-obj-key-src env) 1))
        ret)))


;; Get the index of the fullest thread task queue
(define (get-max-dp-q-idx env)
  (define longest '((queue-length (get-dp-queue env 0)) 0))
  (when (> (get-num-dp-threads env) 1)
    (for ([i (in-range 1 (get-num-dp-threads env))])
         (let ([cur-q-len (queue-length (get-dp-queue env i))])
           (when (> (car longest) cur-q-len)
             (begin
               (set! longest '(cur-q-len i)))))))
  (cdr longest))


;; Get the index of the emptiest thread task queue
(define (get-min-dp-q-idx env)
  (define shortest '((queue-length (get-dp-queue env 0)) 0))
  (when (> (get-num-dp-threads env) 1)
    (for ([i (in-range 1 (get-num-dp-threads env))])
         (let ([cur-q-len (queue-length (get-dp-queue env i))])
           (when (< (car shortest) cur-q-len)
             (begin
               (set! shortest '(cur-q-len i)))))))
  (cdr shortest))


;;Enqueues coroutine instance to the emptiest thread queue and resumes the 
;;thread. The task is typically internally a coroutine
(define (go env coroutine)
  (let ([q-idx (get-min-dp-q-idx env)])
    (semaphore-wait (get-dp-queue-sem env q-idx))
    (enqueue! (get-dp-queue env q-idx) task)
    (semaphore-post (get-dp-queue-sem env q-idx))
    (thread-resume (get-dp-thread env q-idx))
    #t))


;; Hash new data object
(define (hash-dp-data-object env key obj)
  (if (not (get-dp-data-object env key) #f)
      #f ;we've run out of possible hash table values and looped?
      (let ()
        (semaphore-wait (get-dp-data-objects-sem env))
        (hash (get-dp-data-objects env) key obj)
        (semaphore-post (get-dp-data-objects-sem env))
        (coroutine 
          (run-handler)
          (send (get-dp-data-object env key) run))
        (define rh (run-handler))
        (go env rh)
        #t)))


;; Get a data object from the hash
(define (get-dp-data-object env key)
  (hash-ref (get-dp-data-objects env) key #f))


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
(define (set-dp-message-handlers env handlers) 
  (semaphore-wait (get-dp-message-handlers-sem env))
  (vector-set! (vector-ref (get-dp-data env) 1) 3 handlers)
  (semaphore-post (get-dp-message-handlers-sem env)))


(define (get-dp-parent-channel env) 
  (vector-ref (vector-ref (get-dp-data env) 2) 1))


;; Get the semaphore for the parentch-putdp channel
(define (get-dp-parent-ch-sem env)
  (vector-ref (vector-ref (get-dp-data env) 2) 2))


;; Send info from the datapool to the parent
(define (send-to-datapool-parent env form)
  (semaphore-wait (get-dp-parent-ch-sem env))
  (ch-put (get-dp-parent-channel env))
  (semaphore-post (get-dp-parent-ch-sem env)))


;; Return thread's queue index if not empty, otherwise gets the index of the 
;; fullest queue.
(define (get-task-q-idx env thread-idx)
  (let ([thread-queue (get-dp-queue env thread-idx)])
    (if (eqv? (queue-length thread-queue) 0)
        (let ([highest-idx (get-max-dp-q-idx env)])
          (if (eqv? (queue-length (get-dp-queue highest-idx)) 0) 
              (thread-suspend (current-thread))
              (highest-idx)))
        (thread-idx))))


;; Return a task from a thread queue to execute
(define (get-task env thread-idx)
  (let ([q-idx (get-task-q-idx env thread-idx)])
    (semaphore-wait (get-dp-queue-sem env q-idx))
    (if (queue-empty? (get-dp-queue env q-idx))
        (let ()
          (semaphore-post (get-dp-queue-sem env q-idx))
          #f)
        (let ()
          (define task #f)
          (set! task (dequeue! (get-dp-queue env q-idx)))
          (semaphore-post (get-dp-queue-sem env q-idx))
          task))))


;; Evaluate given task, limits cpu starvation by limiting continuous 
;; evaluations in coroutines. If provided task is *not* a coroutine and/or 
;; that coroutine does not (yield) intelligently this may have no effect.
;; Returns: #t if task completed, #f if task not yet completed
(define (dp-thread-exec-task env thread-idx task evals-left)
  (let ([ret (task)])
    (if (task 'dead?) ;check if coroutine is dead
        #t ;task completed
        (if (> evals-left 0)
            (dp-thread-exec-task env thread-idx (- evals-left 1))
            (let ()
              (go env task) ;place task at the back of a queue
              #f))))) ;task not yet completed


;; Eternal thread tail recursion of executing tasks
(define (dp-thread env thread-idx) 
  (let ([task (get-task env thread-idx)])
    (if (eqv? task #f)
        (thread-suspend)
        ;execute the task we get 
        (dp-thread-exec-task
          env 
          thread-idx 
          (get-task env thread-idx) 
          (get-dp-thread-continuous-eval-limit env))))
  (dp-thread thread-idx))


;; Thread startup coroutinetion
(define (dp-thread-start env)
  (let ([pid (current-thread)])
    (thread-suspend pid)
    (define thread-num 0)
    (for ([i (get-num-dp-threads env)])
         (when (eqv? (get-dp-thread i) pid) 
           (set! thread-num i)))
    (dp-thread thread-num)))



;;;----------------------------------------------------------------------------
;;; Create Datapool
;;;---------------------------------------------------------------------------- 
;; Create a datapool environment. Setup worker threads and begin execution of 
;; the user defined main form. Returns datapool environment data vector needed
;; as the argument for management coroutinetions like (close-dp). To kick off 
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
          (let ()
            (semaphore-wait (get-dp-message-handlers-sem))
            (let ([msg-handlers (hash-ref (get-dp-message-handlers)
                                          msg-type 
                                          #f)])
              (if (eqv? #f msg-handlers)
                  (set-dp-message-handlers 
                    (hash-set (get-dp-message-handlers)
                              msg-type 
                              '(callback-form)))
                  (set-dp-message-handlers 
                    (hash-set (get-dp-message-handlers)
                              msg-type 
                              (append msg-handlers '(key callback-form))))))
            (semaphore-post (get-dp-message-handlers-sem))
            #t)
          'deleted))

    ;; Send a message to connected handlers in the current datapool
    (define/private 
      (send-msg msg)
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
                 (coroutine
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
    ;; (coroutine (gimme-3) (+ 1 2))
    ;; (go (set-datum! my-field gimme-3))
    (define-syntax set-datum!
      (syntax-rules ()
        [(set-datum! field val) 
         (if (not (deleted?))
             (let ()
               (coroutine
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
                    (coroutine
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
    (if (not (eqv? obj #f)) ;if object exists
        (let ()
          ;Inform the data object it is deleted. This should stop it from 
          ;executing further (receive-msg), (send-msg), 
          (send obj delete)

          (define (remove-obj-callbacks hkey value) 
            ;Each h is a list of pairs in the form '(dst-obj-key dst-obj-callback)
            (for ([h (length value)])
                 ;Return a list without handler pairs whose dst-obj-key 
                 ;matches key
                 (filter (not (eqv? (car h) obj-key)))))

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
(define (run-unit-tests [wait #f])
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
  (let ()
    (define x 3)
    (test-true? "TEST defined? 1" (if (defined? x) #t #f) wait)
    (test-true? "TEST defined? 2" (if (defined? y) #f #t) wait)
    (test-true? "TEST test-true? 1" #t wait)
    (test-true? "TEST test-true? 2" (not #f) wait)
    (test-equal? "TEST test-equal? 3" 1 1 wait)
    (test-equal? "TEST test-equal? 4" "test" "test" wait)
    (test-equal? "TEST test-equal? 5" 'test 'test wait))
  ;;**************************************


  ;;**************************************
  ;;TEST coroutine
  ;;--------------------------------------
  (let ([arg1 3]
        [arg2 4])

    (set! test-num 1)

    (coroutine
      (co-test1) 
      (yield 1)
      (yield 2)
      #f)

    (define ct1 (co-test1))

    ;Test the coroutine runs correctly
    (test-true? "TEST coroutine alive" (ct1 'alive?) wait)
    (test-equal? (test-text "TEST coroutine") (ct1) 1 wait)
    (test-true? "TEST coroutine alive" (ct1 'alive?) wait)
    (test-equal? (test-text "TEST coroutine") (ct1) 2 wait)
    (test-true? "TEST coroutine alive" (ct1 'alive?) wait)
    (test-true? (test-text "TEST coroutine") (not (ct1)) wait)
    (test-true? "TEST coroutine dead" (ct1 'dead?) wait)

    (coroutine
      (co-test2 arg1 arg2)
      (yield arg1)
      (yield arg2)
      #f)

    (define ct2 (co-test2 "teststring" 'test))

    ;Test coroutine can be run with arguments
    (test-true? "TEST coroutine alive" (ct2 'alive?) wait)
    (test-equal? (test-text "TEST coroutine") (ct2) "teststring" wait)
    (test-true? "TEST coroutine alive" (ct2 'alive?) wait)
    (test-equal? (test-text "TEST coroutine") (ct2) 'test wait)
    (test-true? "TEST coroutine alive" (ct2 'alive?) wait)
    (test-true? (test-text "TEST coroutine") (not (ct2)) wait)
    (test-true? "TEST coroutine dead" (ct2 'dead?) wait)

    (set! ct2 (co-test2 'test2 "teststring2"))

    ;Test coroutine can be rerun with new arguments
    (test-true? "TEST coroutine alive" (ct2 'alive?) wait)
    (test-equal? (test-text "TEST coroutine") (ct2) 'test2 wait)
    (test-true? "TEST coroutine alive" (ct2 'alive?) wait)
    (test-equal? (test-text "TEST coroutine") (ct2) "teststring2" wait)
    (test-true? "TEST coroutine alive" (ct2 'alive?) wait)
    (test-true? (test-text "TEST coroutine") (not (ct2)) wait)
    (test-true? "TEST coroutine dead" (ct2 'dead?) wait)

    ;Should *not* raise an exception due to the stateful (set!)
    (with-handlers ([exn:fail:user? (lambda (e) (test-fail e))])
                   (coroutine 
                     (co-test3 arg1)
                     (yield arg1)
                     (set! arg1 2)
                     (yield arg1))
                   (define ct3 (co-test3 1))
                   (test-equal? (test-text "TEST coroutine") (ct3) 1 wait)
                   (test-equal? (test-text "TEST coroutine") (ct3) 2 wait)
                   ;Final argument of coroutine is a (yield) so we have to run 
                   ;again to hit the end (even though there are no forms left 
                   ;to evaluate)
                   (test-equal? (test-text "TEST coroutine") (ct3) 2 wait)
                   (test-true? "TEST coroutine dead" (ct3 'dead?) wait)))
  ;;************************************** 


  ;;**************************************
  ;;TEST channel ch-get ch-put
  ;;--------------------------------------
  (let ([ch (channel)])
    (test-true? "TEST channel creation" (async-channel? ch)) wait)

  (let ([ch (channel)])
    (async-channel-put ch "teststring")
    (let ([ret (ch-get ch #f)])
      (test-equal? "TEST ch-get can get a string" ret "teststring")) wait)

  (let ([ch (channel)])
    (async-channel-put ch "teststring2")
    (let ([ret (ch-get ch)])
      (test-equal? "TEST ch-get can get a different string" 
                   ret 
                   "teststring2") wait))

  (let ([ch (channel)])
    (ch-put ch "teststring")
    (let ([ret (ch-get ch #f)])
      (test-equal? "TEST ch-put a string" ret "teststring")) wait)

  (let ([ch (channel)])
    (ch-put ch "teststring2")
    (let ([ret (ch-get ch)])
      (test-equal? "TEST ch-put another string" ret "teststring2")) wait)
  ;;**************************************


  ;;**************************************
  ;;TEST make-dp-data 
  ;;     get-dp-data 
  ;;     close-dp
  ;;--------------------------------------
  ;Sanity test
  (test-equal? "" (get-dp-data) #f wait)

  ;Make a datapool
  (let ([num-threads 2]
        [*datapool-environment-data* (make-dp-data 2)])

    ;Verify threads exist
    (for ([i num-threads])
         (test-true? "" 
                     (thread? 
                       (vector-ref (vector-ref (vector-ref (get-dp-data) 0) i) 0)) wait))

    ;Verify the threads are alive
    (for ([i num-threads])
         (test-true? "" (not (thread-dead? 
                               (vector-ref
                                 (vector-ref 
                                   (vector-ref (get-dp-data) 0) 
                                   i) 0))) wait))

    ;Verify task queues exist
    (for ([i num-threads])
         (test-true? "" 
                     (queue? 
                       (vector-ref (vector-ref (vector-ref (get-dp-data) 0) i) 1)) wait))

    ;Verify task queue semaphores exist
    (for ([i num-threads])
         (test-true? "" 
                     (semaphore? 
                       (vector-ref (vector-ref (vector-ref (get-dp-data) 0) i) 2)) wait))

    ;Verify hash table of message handlers exists
    (test-true? "" 
                (hash? 
                  (vector-ref (vector-ref (get-dp-data) 1) 0)) wait)

    ;Verify hash table semaphore exists
    (test-true? "" 
                (semaphore? 
                  (vector-ref (vector-ref (get-dp-data) 1) 1)) wait)

    ;Verify parentch-putdp channel exists
    (test-true? "" 
                (async-channel? 
                  (vector-ref (vector-ref (get-dp-data) 2) 0)) wait)

    ;Verify dpch-putparent channel exists
    (test-true? "" 
                (async-channel? 
                  (vector-ref (vector-ref (get-dp-data) 2) 1)) wait)

    ;Verify we can kill the datapool environment
    (close-dp *datapool-environment-data*)

    (for ([i num-threads])
         (test-true? "" (thread-dead? 
                          (vector-ref 
                            (vector-ref 
                              (vector-ref (get-dp-data) 0) i) 0)) wait)))
  ;;**************************************


  ;;**************************************
  ;;TEST get-datapool-channel 
  ;;     get-num-dp-threads 
  ;;     get-dp-thread 
  ;;     get-dp-queue 
  ;;     get-dp-queue-sem 
  ;;     get-dp-data-objects 
  ;;     get-dp-data-objects-sem 
  ;;     get-dp-data-objects 
  ;;     get-dp-data-objects-sem 
  ;;     gen-dp-data-obj-key
  ;;--------------------------------------
  (let ([num-threads 2])
    (let 
      ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
       [*datapool-environment-data* (make-dp-data num-threads)]
       [*data-obj-key-src* 0])

      ;Check that parentch-putdp channel exists
      (test-true? "" (async-channel? 
                       (get-datapool-channel *datapool-environment-data*)) wait)

      ;Check correct num of threads exist 
      (test-equal? "" (get-num-dp-threads) 2 wait)

      ;Check threads exist 
      (test-true? "" (thread? (get-dp-thread 0)) wait)
      (test-true? "" (thread? (get-dp-thread 1)) wait)

      ;Check thread task queues exist
      (test-true? "" (queue? (get-dp-queue 0)) wait)
      (test-true? "" (queue? (get-dp-queue 1)) wait)

      ;Check thread task queues semaphores exist
      (test-true? "" (semaphore? (get-dp-queue-sem 0)) wait)
      (test-true? "" (semaphore? (get-dp-queue-sem 1)) wait)

      ;Check data object hash exists
      (test-true? "" (hash? (get-dp-data-objects)) wait)
      (test-true? "" (semaphore? (get-dp-data-objects-sem)) wait)

      ;Check key generation coroutinetion
      (test-equal? "" (gen-dp-data-obj-key) 0 wait)
      (test-equal? "" (gen-dp-data-obj-key) 1 wait)
      (test-equal? "" (gen-dp-data-obj-key) 2 wait)
      (test-equal? "" (gen-dp-data-obj-key) 3 wait)
      (close-dp *datapool-environment-data*)))
  ;;**************************************

  ;;**************************************
  ;;TEST get-min-dp-q-idx 
  ;;     get-max-dp-q-idx 
  ;;     go
  ;;--------------------------------------
  (let ([num-threads 2])
    (let
      ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
       [*datapool-environment-data* (make-dp-data num-threads)]
       [*data-obj-key-src* 0])

      ;;Arbitrary coroutinetion to execute
      (define (test-task) #t)

      ;;Test defaults
      ;q0-size: 0, q1-size: 0
      (test-equal? "" (get-min-dp-q-idx) 0 wait)
      (test-equal? "" (get-max-dp-q-idx) 0 wait)
      (test-equal? "" (queue-length (get-dp-queue 0)) 0 wait)
      (test-equal? "" (queue-length (get-dp-queue 1)) 0 wait)

      (enqueue! (get-dp-queue 0) test-task)

      ;q0-size: 1, q1-size: 0
      (test-equal? "" (get-min-dp-q-idx) 1 wait)
      (test-equal? "" (get-max-dp-q-idx) 0 wait)
      (test-equal? "" (queue-length (get-dp-queue 0)) 1 wait)
      (test-equal? "" (queue-length (get-dp-queue 1)) 0 wait)

      (enqueue! (get-dp-queue 1) test-task)

      ;q0-size: 1, q1-size: 1
      (test-equal? "" (get-min-dp-q-idx) 0 wait)
      (test-equal? "" (get-max-dp-q-idx) 0 wait)
      (test-equal? "" (queue-length (get-dp-queue 0)) 1 wait)
      (test-equal? "" (queue-length (get-dp-queue 1)) 1 wait)

      (enqueue! (get-dp-queue 1) test-task)

      ;q0-size: 1, q1-size: 2
      (test-equal? "" (get-min-dp-q-idx) 0 wait)
      (test-equal? "" (get-max-dp-q-idx) 1 wait)
      (test-equal? "" (queue-length (get-dp-queue 0)) 1 wait)
      (test-equal? "" (queue-length (get-dp-queue 1)) 2 wait)

      ;resume thread execution
      (go test-task) 
      (sleep 1)

      ;threads should be completed and asleep again
      ;q0-size: 0, q1-size: 0
      (test-equal? "" (get-min-dp-q-idx) 0 wait)
      (test-equal? "" (get-max-dp-q-idx) 0 wait)
      (test-equal? "" (queue-length (get-dp-queue 0)) 0 wait)
      (test-equal? "" (queue-length (get-dp-queue 1)) 0 wait)

      (close-dp *datapool-environment-data*)))
  ;;**************************************


  ;;**************************************
  ;;TEST hash-dp-data-object
  ;;     get-dp-data-object
  ;;     delete-dp-data-object
  ;;--------------------------------------
  (let ([num-threads 2])
    (let 
      ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
       [*datapool-environment-data* (make-dp-data num-threads)]
       [*data-obj-key-src* 0])

      (define test-class%
        (class object% (super-new)
               (define/public (get-3)
                              3)))

      (define test-object (make-object test-class%))

      (test-equal? "" (hash-count (get-dp-data-objects)) 0 wait)
      (test-true? "" (hash-dp-data-object 0 test-object) wait)
      (test-true? "" (not (hash-dp-data-object 0 test-object)) wait)

      (test-equal? "" (send (get-dp-data-object 0) get-3) 3 wait)
      (test-true? "" (not (get-dp-data-object 1)) wait)

      (test-true? "" (delete-dp-data-object 0) wait)
      (test-true? "" (not (get-dp-data-object 0)) wait)))
  ;;**************************************


  ;;**************************************
  ;;TEST get-dp-message-handlers
  ;;     get-dp-message-handlers-sem
  ;;     set-dp-message-handlers
  ;;--------------------------------------
  (let ([num-threads 2])
    (let 
      ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
       [*datapool-environment-data* (make-dp-data num-threads)]
       [*data-obj-key-src* 0])
      (test-true? "" (hash? (get-dp-message-handlers)) wait)
      (test-true? "" (semaphore? (get-dp-message-handlers-sem)) wait)

      (define test-type 'test-type)
      (define (callback-form) 3)
      ;Probably should rework this coroutineion to be simpler. However, it's not exposed
      ;by the module and should only be used internally so it's probably fine for
      ;now
      (set-dp-message-handlers
        (hash-set (get-dp-message-handlers)
                  test-type
                  '(callback-form)))

      (test-equal? "" (hash-ref (get-dp-message-handlers) test-type) '(callback-form) wait)

      (define (callback-form-2) 2)
      (let ([cur-handlers (hash-ref (get-dp-message-handlers) test-type)])
        (set-dp-message-handlers
          (hash-set (get-dp-message-handlers)
                    test-type
                    (append cur-handlers '(callback-form-2)))))

      (test-equal? "" (hash-ref (get-dp-message-handlers) test-type) 
                   '(callback-form callback-form-2) wait)))
  ;;**************************************

  ;;**************************************
  ;;TEST get-dp-parent-channel
  ;;     get-dp-parent-ch-sem
  ;;     send-to-datapool-parent 
  ;;     go ;from parent
  ;;--------------------------------------
  (let ([num-threads 2])
    (let 
      ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
       [*datapool-environment-data* (make-dp-data num-threads)]
       [test-dp-data *datapool-environment-data*]
       [*data-obj-key-src* 0])
      (test-equal? "" (channel? (get-dp-parent-channel)) wait)
      (test-equal? "" (semaphore? (get-dp-parent-ch-sem)) wait)

      (ch-put (get-dp-parent-channel) #t)
      (test-true? "" (ch-get (get-dp-parent-channel)) wait)

      (ch-put (get-dp-parent-channel) "test-string" wait)
      (test-equal? "" (ch-get (get-dp-parent-channel)) "test-string" wait)

      (send-to-datapool-parent #t)
      (test-true? "" (ch-get (get-dp-parent-channel)) wait)

      (send-to-datapool-parent "test-string2")
      (test-equal? "" (ch-get (get-dp-parent-channel)) "test-string2" wait)

      (let ([ch (channel)])
        (coroutine (test-coroutine)
                   (ch-put ch 'complete))
        (go test-dp-data test-coroutine)
        (sleep 1)
        (test-equal? "" (ch-get ch) 'complete wait))))

  ;;**************************************

  ;;**************************************
  ;;TEST get-task-q-idx
  ;;     get-task
  ;;     dp-thread-exec-task
  ;;     dp-thread
  ;;     dp-thread-start
  ;;--------------------------------------
  (let ([num-threads 2])
    (let
      ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
       [*datapool-environment-data* (make-dp-data num-threads)]
       [*data-obj-key-src* 0])
      (test-equal? "" (get-task-q-idx 0) 0 wait)
      (test-equal? "" (get-task-q-idx 1) 1 wait)

      (define (test-task) #t)
      (enqueue! (get-dp-queue 1) test-task)

      (test-equal? "" (get-task-q-idx 0) 1 wait)
      (test-equal? "" (get-task-q-idx 1) 1 wait)

      (enqueue! (get-dp-queue 0) test-task)

      (test-equal? "" (get-task-q-idx 0) 0 wait)
      (test-equal? "" (get-task-q-idx 1) 1 wait)

      (enqueue! (get-dp-queue 0) test-task)

      (test-equal? "" (get-task-q-idx 0) 0 wait)
      (test-equal? "" (get-task-q-idx 1) 0 wait)

      (let ([len-0 (queue-length (get-dp-queue 0))]
            [task (get-task 0)])
        (test-true? "" (> (queue-length (get-dp-queue 0))) wait)
        (test-true? "" (> len-0 (queue-length (get-dp-queue))) wait)

        (test-true? "" (dp-thread-exec-task 0 task 10) wait)

        ;coroutine coroutine that evaluates infinitely
        (coroutine 
          (eval-infinity) 
          (eval-infinity))

        ;Should evaluate 10 times then return #f
        (test-true? "" (not (dp-thread-exec-task 0 eval-infinity 10)) wait))

      ;Don't need to sleep as the current thread is actually running this coroutine
      (dp-thread 1)
      (test-true? "" (not (thread-dead? (get-dp-thread 1))) wait)
      (test-true? "" (not (thread-running? (get-dp-thread 1))) wait)
      (test-true? "" (thread-running? (current-thread)) wait)
      (test-equal? "" (queue-length (get-dp-queue 0) 0) wait)
      (test-equal? "" (queue-length (get-dp-queue 1) 0) wait)

      (enqueue! (get-dp-queue 1) test-task)
      (enqueue! (get-dp-queue 1) test-task)

      (test-equal? "" (queue-length (get-dp-queue 1) 2) wait)

      ;Don't need to sleep as the current thread is actually running this coroutine
      (dp-thread-start)
      (test-equal? "" (queue-length (get-dp-queue 0) 0) wait)
      (test-equal? "" (queue-length (get-dp-queue 1) 0) wait)))
  ;;**************************************

  ;;**************************************
  ;;TEST datapool
  ;;--------------------------------------
  (let ()
    (coroutine (main) 
               (let ([parent (get-dp-parent-channel)])
                 (ch-put parent "test string")))

    (define dp (datapool 2 main))
    (sleep 1)

    (test-equal? "" (ch-get (get-datapool-channel dp)) "test string" wait)
    (close-dp dp))
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
  ;(let ([ch (get-datapool-channel dp1)]) 
  ;(ch-get ch))


  ;;;---------------------------------------------------------------------------- 
  ;;; Closing Analysis
  ;;;---------------------------------------------------------------------------- 
  (exit)
  )
