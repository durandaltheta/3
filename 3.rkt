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
         definep ;define a pure & stateless function
         func ;return a coroutine thunk for use in (go)
         ;danger-func! ;define a coroutine with no state checks
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
(define (test-true? description form [wait #f])
  (if (eqv? form #t)
      (let ()
        (printf "pass ~a: ~a == #t\n\n" description form)
        #t)
      (let ()
        (printf "FAIL ~a: ~a != #f\n\n" description form)
        (when wait
          (read-line (current-input-port) 'any))
        #f)))


;; Return #t if quoted forms return an equal value, else #f
(define (test-equal? description form-a form-b [wait #f])
  (if (eqv? form-a form-b)
      (let ()
        (printf "pass ~a: ~a == ~a\n\n" description form-a form-b)
        #t)
      (let ()
        (printf "FAIL ~a: ~a != ~a\n\n" description form-a form-b)
        (when wait
          (read-line (current-input-port) 'any))
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


;; Define a coroutine of a pure stateless coroutine. These should always 
;; be safe when executed asynchronously with a (go) call. The current 
;; 'coroutine' portion of the definition *is* stateful, only the provided form 
;; is guaranteed to be stateless 
#|
(define-syntax-rule (func (name . params) body ...)
                    (eval (let ()
                            ;Special check to see if given function symbols 
                            ;produce a pure & stateless function. Otherwise
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
                                      ...)))
|#

(define-syntax (func stx)
  (syntax-case stx ()
               ((_ (name . args) . body )
                #'(define-coroutine (name . args)
                    (yield 'alive)
                    . body))))


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
;;; Datapool Data Functions
;;;--------------------------------------------------------------------------
;;create datapool data 
(define (make-dp-data num-threads) 
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
        (channel #f) ;parentch-putdatapool channel
        (channel #f) ;datapoolch-putparent channel
        (make-semaphore))))) ;datapoolch-putparent channel semaphore


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


;; Get the communication parentch-putdatapool channel
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
  (vector-ref (vector-ref (vector-ref (get-dp-data) 0) idx) 1))


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


;;Enqueues task expresion to the emptiest thread queue and resumes the 
;;thread. The task is typically internally a coroutine
(define (go task)
  (let ([q-idx (get-min-dp-q-idx)])
    (semaphore-wait (get-dp-queue-sem q-idx))
    (enqueue! (get-dp-queue q-idx) task)
    (semaphore-post (get-dp-queue-sem q-idx))
    (thread-resume (get-dp-thread q-idx))
    #t))


;;Apply (go form) to given task within the given dp-data environment. 
;;This works because dp-data is a box that accesses the internally stored 
;;datapool environment data "by reference"
(define (go-dp dp-data task)
  (let ([*datapool-environment-data* dp-data])
    (go task)))


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
  (semaphore-post (get-dp-data-objects-sem))
  #t)


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


(define (get-dp-parent-channel) 
  (vector-ref (vector-ref (get-dp-data) 2) 1))


;; Get the semaphore for the parentch-putdp channel
(define (get-dp-parent-ch-sem)
  (vector-ref (vector-ref (get-dp-data) 2) 2))


;; Send info from the datapool to the parent
(define (send-to-datapool-parent form)
  (semaphore-wait (get-dp-parent-ch-sem))
  (ch-put (get-dp-parent-channel))
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
  (let ([q-idx (get-task-q-idx thread-idx)])
    (semaphore-wait (get-dp-queue-sem q-idx))
    (if (queue-empty? (get-dp-queue q-idx))
        (let ()
          (semaphore-post (get-dp-queue-sem q-idx))
          #f)
        (let ()
          (define task #f)
          (set! task (dequeue! (get-dp-queue q-idx)))
          (semaphore-post (get-dp-queue-sem q-idx))
          task))))


;; Evaluate given task, limits cpu starvation by limiting continuous 
;; evaluations in coroutines. If provided task is *not* a coroutine and/or 
;; that coroutine does not (yield) intelligently this may have no effect.
;; Returns: #t if task completed, #f if task not yet completed
(define (dp-thread-exec-task thread-idx task evals-left)
  (let ([ret (task)])
    (if (eqv? ret 'alive)
        (if (> evals-left 0)
            (dp-thread-exec-task thread-idx (- evals-left 1))
            (begin 
              (go task) ; place task at the back of a queue
              #f)) ; task not yet completed
        #t))) ; task completed


;; Eternal thread tail recursion of executing tasks
(define (dp-thread thread-idx) 
  (let ([task (get-task thread-idx)])
    (if (eqv? task #f)
        (thread-suspend)
        ;execute the task we get 
        (dp-thread-exec-task 
          thread-idx 
          (get-task thread-idx) 
          *dp-thread-continuous-eval-limit*)))
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
(define (datapool num-threads dp-main)
  (let ([*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
        [*datapool-environment-data* (make-dp-data num-threads)]
        [*data-obj-key-src* 0]
        [*data-obj-free-key-q* (make-queue)])
    (if (> (get-num-dp-threads) 0)
        (let ()
          (go dp-main) ;Execute the provided dp-main function
          #t)
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
  (define test-num 0)
  (define (test-text text)
    (let ([ret (string-append text (number->string test-num))])
      (set! test-num (+ test-num 1))
      ret))

  ;;;---------------------------------------------------------------------------- 
  ;;; Function Tests
  ;;;---------------------------------------------------------------------------- 
  ;;**************************************
  ;;TEST defined?
  ;;     test-true? 
  ;;     test-equal?
  ;;     test-pass
  ;;     test-fail
  ;;--------------------------------------
  (let ()
    (define x 3)
    (test-true? "TEST defined? 0" (if (defined? x) #t #f) wait)
    (test-true? "TEST defined? 1" (if (defined? y) #f #t) wait)
    (test-true? "TEST test-true? 0" #t wait)
    (test-true? "TEST test-true? 1" (not #f) wait)
    (test-equal? "TEST test-equal? 0" 1 1 wait)
    (test-equal? "TEST test-equal? 1" "test" "test" wait)
    (test-equal? "TEST test-equal? 2" 'test 'test wait)
    ;Comparing lists apparently isn't part of eqv? or equal?
    ;(test-equal? "Is a list equal to itself?" '("test" "test2") '("test" "test2"))
    ;(test-pass "TEST test-pass" "pass text")
    ;(test-fail "TEST test-fail' work?" "fail text"))
    )
  ;;**************************************


  ;;**************************************
  ;;TEST define-coroutine 
  ;;--------------------------------------
  (let ()
    (set! test-num 0)
    (define-coroutine
      (co-test1) 
      (yield 1)
      (yield 2))

    (define co-test1-symbol (co-test1))


    ;Test the coroutine runs correctly
    (test-equal? (test-text "TEST define-coroutine ") (co-test1-symbol) 1 wait)
    (test-equal? (test-text "TEST define-coroutine ") (co-test1-symbol) 2 wait)

    (define-coroutine
      (co-test2 arg1 arg2)
      (yield arg1)
      (yield arg2)
      #f)

    (define co-test2-symbol (co-test2 "teststring" 'test))

    ;Test coroutine can be run with arguments
    (test-equal? (test-text "TEST define-coroutine ") (co-test2-symbol)  "teststring" wait)
    (test-equal? (test-text "TEST define-coroutine ") (co-test2-symbol)  'test wait)
    (test-true? (test-text "TEST define-coroutine ") (not (co-test2-symbol)) wait)

    (set! co-test2-symbol (co-test2 'test2 "teststring2"))

    ;Test coroutine can be rerun with new arguments
    (test-equal? (test-text "TEST define-coroutine ") (co-test2-symbol)  'test2 wait)
    (test-equal? (test-text "TEST define-coroutine ") (co-test2-symbol)  "teststring2" wait)
    (test-true? (test-text "TEST define-coroutine ") (not (co-test2-symbol))) wait)
  ;;**************************************


  ;;**************************************
  ;;TEST func
  ;;--------------------------------------
  (let ([arg1 3]
        [arg2 4])

    (set! test-num 0)

    (func
      (co-test1) 
      (yield 1)
      (yield 2)
      #f)

    (define ct1 (co-test1))

    (display "!!!!!!!! ~a\n" co-test1)
    (display "!!!!!!!! ~a\n" ct1)

    ;Test the coroutine runs correctly
    (test-equal? (test-text "TEST Func ") (ct1) 'alive wait)
    (test-equal? (test-text "TEST Func ") (ct1) 1 wait)
    (test-equal? (test-text "TEST Func ") (ct1) 2 wait)
    (test-true? (test-text "TEST Func ") (not (ct1)) wait)

    (func
      (co-test2 arg1 arg2)
      (yield arg1)
      (yield arg2)
      #f)

    (define ct2 (co-test2 "teststring" 'test))

    ;Test coroutine can be run with arguments
    (test-equal? (test-text "TEST Func ") (ct2) 'alive wait)
    (test-equal? (test-text "TEST Func ") (ct2) "teststring" wait)
    (test-equal? (test-text "TEST Func ") (ct2) 'test wait)
    (test-true? (test-text "TEST Func ") (not (ct2)) wait)

    (set! ct2 (co-test2 'test2 "teststring2"))

    ;Test coroutine can be rerun with new arguments
    (test-equal? (test-text "TEST Func ") (ct2) 'alive wait)
    (test-equal? (test-text "TEST Func ") (ct2) 'test2 wait)
    (test-equal? (test-text "TEST Func ") (ct2) "teststring2" wait)
    (test-true? (test-text "TEST Func ") (ct2) wait)

    ;Should *not* raise an exception due to the stateful (set!)
    (with-handlers ([exn:fail:user? (lambda (e) (test-fail e))])
                   (func 
                     (co-test3 arg1)
                     (yield arg1)
                     (set! arg1 2)
                     (yield arg2))
                   (define ct3 (co-test3 1))
                   (test-equal? (test-text "TEST Func ") (ct3) 'alive wait)
                   (test-equal? (test-text "TEST Func ") (ct3) 1 wait)
                   (test-equal? (test-text "TEST Func ") (ct3) 2 wait)
                   (test-true? (test-text "TEST Func ") (ct3))) wait)
  ;;**************************************


  ;;**************************************
  ;;TEST channel
  ;;--------------------------------------
  (let ([ch (channel)])
    (test-true? "" (async-channel? ch)) wait)
  ;;**************************************


  ;;**************************************
  ;;TEST ch-get
  ;;--------------------------------------
  (let ([ch (channel)])
    (async-channel-put ch "teststring")
    (let ([ret (ch-get ch #f)])
      (test-equal? "" ret "teststring")) wait)

  (let ([ch (channel)])
    (async-channel-put ch "teststring2")
    (let ([ret (ch-get ch)])
      (test-equal? "" ret "teststring2")) wait)
  ;;**************************************


  ;;**************************************
  ;;TEST ch-put
  ;;--------------------------------------
  (let ([ch (channel)])
    (ch-put ch "teststring")
    (let ([ret (ch-get ch #f)])
      (test-equal? "" ret "teststring")) wait)

  (let ([ch (channel)])
    (ch-put ch "teststring2")
    (let ([ret (ch-get ch)])
      (test-equal? "" ret "teststring2")) wait)
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

      ;Check key generation Function
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

      ;;Arbitrary function to execute
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
      ;Probably should rework this funcion to be simpler. However, it's not exposed
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
  ;;     go-dp
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
        (func (test-func)
                   (ch-put ch 'complete))
        (go-dp test-dp-data test-func)
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

        ;func coroutine that evaluates infinitely
        (func 
          (eval-infinity) 
          (eval-infinity))

        ;Should evaluate 10 times then return #f
        (test-true? "" (not (dp-thread-exec-task 0 eval-infinity 10)) wait))

      ;Don't need to sleep as the current thread is actually running this func
      (dp-thread 1)
      (test-true? "" (not (thread-dead? (get-dp-thread 1))) wait)
      (test-true? "" (not (thread-running? (get-dp-thread 1))) wait)
      (test-true? "" (thread-running? (current-thread)) wait)
      (test-equal? "" (queue-length (get-dp-queue 0) 0) wait)
      (test-equal? "" (queue-length (get-dp-queue 1) 0) wait)

      (enqueue! (get-dp-queue 1) test-task)
      (enqueue! (get-dp-queue 1) test-task)

      (test-equal? "" (queue-length (get-dp-queue 1) 2) wait)

      ;Don't need to sleep as the current thread is actually running this func
      (dp-thread-start)
      (test-equal? "" (queue-length (get-dp-queue 0) 0) wait)
      (test-equal? "" (queue-length (get-dp-queue 1) 0) wait)))
  ;;**************************************

  ;;**************************************
  ;;TEST datapool
  ;;--------------------------------------
  (let ()
    (func (main) 
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
