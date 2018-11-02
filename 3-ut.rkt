;;**************************************
;; Test Prep
;;--------------------------------------

;; Calculate iterations per second 
(: iterations-per-second (-> Number Real))
(define (iterations-per-second milli iter) (/ iter (/ milli 1000)))
;;**************************************



;;**************************************
;;TEST defined?
;;     test-true? 
;;     test-equal?
;;     test-pass
;;     test-fail
;;-------------------------------------- 
(: test-test-functions (-> Void))
(define (test-test-functions)
  (test-section "test functions")
  (let ()
    (: x Integer)
    (define x 3)
    (test-true? "defined? 1" (if (defined? x) #t #f) pr wait)
    (test-true? "defined? 2" (if (defined? y) #f #t) pr wait)
    (test-true? "test-true? 1" #t pr wait)
    (test-true? "test-true? 2" (not #f) pr wait)
    (test-equal? "test-equal? 3" 1 1 pr wait)
    (test-equal? "test-equal? 4" "test" "test" pr wait)
    (test-equal? "test-equal? 5" 'test 'test pr wait))
  (void))
;;**************************************


;;**************************************
;;TEST coroutine
;;--------------------------------------
(: test-coroutines (-> Void))
(define (test-coroutines)
  (test-section "coroutines")
  (let ([arg1 3]
        [arg2 4])

    (: test-num Integer)
    (define test-num 1)

    (: co-test1 (-> Any))
    (define-coroutine
      (co-test1) 
      (yield 1)
      (yield 2)
      #f)

    (: ct1 (-> Any))
    (define ct1 (co-test1))

    ;Test the coroutine runs correctly
    (test-true? "new coroutine alive" (ct1 'suspended?) pr wait)
    (test-equal? "coroutine yields expected 1" (ct1) 1 pr wait)
    (test-true? "coroutine alive" (ct1 'suspended?) pr wait)
    (test-equal? "coroutine yields expected 2" (ct1) 2 pr wait)
    (test-true? "coroutine alive" (ct1 'suspended?) pr wait)
    (test-true? "coroutine returns #f" (not (ct1)) pr wait)
    (test-true? "coroutine dead" (ct1 'dead?) pr wait)

    (: co-test2 (-> Any))
    (define-coroutine
      (co-test2 arg1 arg2)
      (yield arg1)
      (yield arg2)
      #f)

    (: ct2 (-> Any))
    (define ct2 (co-test2 "teststring" 'test))

    ;Test coroutine can be run with arguments
    (test-true? "new coroutine alive" (ct2 'suspended?) pr wait)
    (test-equal? "coroutine yields \"teststring\"" (ct2) "teststring" pr wait)
    (test-true? "coroutine alive" (ct2 'suspended?) pr wait)
    (test-equal? "coroutine yields 'test" (ct2) 'test pr wait)
    (test-true? "coroutine alive" (ct2 'suspended?) pr wait)
    (test-true? "coroutine returns #f" (not (ct2)) pr wait)
    (test-true? "coroutine dead" (ct2 'dead?) pr wait)

    (set! ct2 (co-test2 'test2 "teststring2"))

    ;Test coroutine can be rerun with new arguments
    (test-true? "new coroutine alive" (ct2 'suspended?) pr wait)
    (test-equal? "coroutine yields 'test" (ct2) 'test2 pr wait)
    (test-true? "coroutine alive" (ct2 'suspended?) pr wait)
    (test-equal? "coroutine yields \"teststring2\"" (ct2) "teststring2" pr wait)
    (test-true? "coroutine alive" (ct2 'suspended?) pr wait)
    (test-true? "coroutine" (not (ct2)) pr wait)
    (test-true? "coroutine dead" (ct2 'dead?) pr wait)

    ;Should *not* raise an exception due to the stateful (set!)
    (with-handlers ([exn:fail:user? (lambda (e) (test-fail e))])
                   (: co-test3 (-> Any Any))
                   (define-coroutine 
                     (co-test3 arg1)
                     (yield arg1)
                     (set! arg1 2)
                     (yield arg1))
                   (: ct3 (-> Any))
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
    (: co-test4 (-> Any Any))
    (define-coroutine (co-test4 val)
                      (define (recurse-func val)
                        (if (equal? val 0)
                            val
                            (let ()
                              (yield val)
                              (recurse-func (- val 1)))))
                      (recurse-func val))

    (: ct4 (-> Any))
    (define ct4 (co-test4 3))
    (test-true? "new coroutine alive" (ct4 'suspended?) pr wait)
    (test-equal? "recursive function in coroutine yields expected value" (ct4) 3 pr wait)
    (test-equal? "recursive function in coroutine yields expected value" (ct4) 2 pr wait)
    (test-equal? "recursive function in coroutine yields expected value" (ct4) 1 pr wait)
    (test-equal? "recursive function in coroutine returns expected value" (ct4) 0 pr wait)
    (test-true? "coroutine dead" (ct4 'dead?) pr wait))
  (void))
;;************************************** 


;;**************************************
;;TEST channel ch-get ch-put
;;--------------------------------------
(: test-channels (-> Void))
(define (test-channels)
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
  (void))
;;**************************************


;;**************************************
;;TEST datapool 
;;     unbox-dp-env 
;;     close-dp
;;-------------------------------------- 
(: test-datapool-intern (-> Void))
(define (test-datapool-intern)
  (test-section "datapool data constructor, getter, and destructor functions")
  ;Make a datapool
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)]) 

    ;;------------------------------------- 
    ;; check num counts are correct
    (test-equal?
      "number of threads"
      (vector-ref (vector-ref (vector-ref (unbox-dp-env env) 0) 0) 0)
      8
      pr
      wait)

    ;;-------------------------------------
    ;Verify threads exist
    (for ([i num-threads])
         (test-true? 
           "(and datapool get-data) verify threads exist" 
           (thread? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (vector-ref 
                     (unbox-dp-env env) 
                     0)
                   1) 
                 i) 
               0)) 
           pr 
           wait))

    ;Verify the threads are alive
    (for ([i num-threads])
         (test-true? 
           "(and datapool get-data) verify threads are alive" 
           (not 
             (thread-dead? 
               (vector-ref
                 (vector-ref 
                   (vector-ref 
                     (vector-ref 
                       (unbox-dp-env env) 
                       0)
                     1) 
                   i) 
                 0))) 
           pr 
           wait))

    ;Verify thread task queues exist
    (for ([i num-threads])
         (test-true? 
           "(and datapool get-data) verify task queues exist" 
           (queue? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (vector-ref
                     (unbox-dp-env env) 
                     0)
                   1) 
                 i) 
               1)) 
           pr 
           wait))

    ;Verify thread task queue semaphores exist
    (for ([i num-threads])
         (test-true? 
           "(and datapool get-data) verify task queue semaphores exist" 
           (semaphore? 
             (vector-ref 
               (vector-ref 
                 (vector-ref 
                   (vector-ref 
                     (unbox-dp-env env) 
                     0)
                   1) 
                 i) 
               2)) 
           pr 
           wait))

    ;;-------------------------------------
    ;Verify hash table of message handlers exists
    (test-true? 
      "(and datapool get-data) verify message handler hash table exists" 
      (hash? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env env) 
            1) 
          0)) 
      pr 
      wait)

    ;Verify message hash table semaphore exists
    (test-true? 
      "(and datapool get-data) verify message handlers hash table semaphore exists" 
      (semaphore? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env env) 
            1) 
          1)) 
      pr 
      wait)

    ;;-------------------------------------
    ;Verify hash table of data objects exists
    (test-true? 
      "(and datapool get-data) verify hash table of data objects exists" 
      (hash? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env env) 
            2) 
          0)) 
      pr 
      wait)

    ;Verify hash table semaphore exists
    (test-true? 
      "(and datapool get-data) verify data object hash table semaphore exists" 
      (semaphore? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env env) 
            2) 
          1)) 
      pr 
      wait)

    ;Verify data object key source variable exists
    (test-true? 
      "(and datapool get-data) verify data object key source variable exists" 
      (number? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env env) 
            2) 
          2)) 
      pr 
      wait)

    ;Verify data object freed key queue exists
    (test-true? 
      "(and datapool get-data) verify data object freed key queue exists" 
      (queue? 
        (vector-ref 
          (vector-ref 
            (unbox-dp-env env) 
            2) 
          3)) 
      pr 
      wait)

    ;Verify we can kill the datapool environment
    (close-dp env #t #t)

    (for ([i num-threads])
         (let ([o (open-output-string)])
           (fprintf o "close-dp thread ~a" i)
           (test-true? 
             (get-output-string o) 
             (thread-dead? 
               (vector-ref 
                 (vector-ref 
                   (vector-ref 
                     (vector-ref 
                       (unbox-dp-env env) 
                       0)
                     1) 
                   i) 
                 0)) 
             pr 
             wait))))
  (void))
;;**************************************


;;**************************************
;;TEST get-cp-thread 
;;     get-cp-queue 
;;     get-cp-queue-sem 
;;     get-data-hash 
;;     get-data-sem 
;;     gen-dp-data-obj-key 
;;     get-data-key-src 
;;     set-data-key-src 
;;     get-data-free-key-q
;;--------------------------------------
(: test-datapool-getters-setters (-> Void))
(define (test-datapool-getters-setters)
  (test-section "datapool getters & setters")
  (let* ([num-threads 2]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])

    ;-------------------------------------- 
    ;verify datapool info is correct
    (test-equal?
      "get-num-dp-threads"
      (get-num-dp-threads env)
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
      "get-cp-thread verify threads exist 1" 
      (thread? (get-cp-thread (get-computepool env) 0)) 
      pr 
      wait)

    (test-true? 
      "get-cp-thread verify threads exist 2" 
      (thread? (get-cp-thread (get-computepool env) 1)) 
      pr 
      wait)

    ;verify thread task queues exist
    (test-true? 
      "get-cp-queue verify thread task queues exist 1" 
      (queue? (get-cp-queue (get-computepool env) 0)) 
      pr 
      wait)

    (test-true? 
      "get-cp-queue verify thread task queues exist 2" 
      (queue? (get-cp-queue (get-computepool env) 1)) 
      pr 
      wait)

    ;verify thread task queues semaphores exist
    (test-true? 
      "get-cp-queue-sem verify thread task queues semaphores exist 1" 
      (semaphore? (get-cp-queue-sem (get-computepool env) 0)) 
      pr 
      wait)

    (test-true? 
      "get-cp-queue-sem verify thread task queues semaphores exist 2" 
      (semaphore? (get-cp-queue-sem (get-computepool env) 1)) 
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

    (close-dp env #t #t))
  (void))
;;**************************************


;;**************************************
;;TEST get-min-cp-q-idx 
;;     get-max-cp-q-idx 
;;     go
;;-------------------------------------- 
(: test-task-queues (-> Void))
(define (test-task-queues)
  (test-section "task queue getters & setters")
  (let* ([num-threads 2]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])

    ;;Wait for worker threads to put themselves to sleep
    (sleep 0.1)

    ;;Arbitrary coroutine to execute 
    (: test-task (-> True))
    (define-coroutine (test-task) #t)

    ;;Test defaults
    ;q0-size: 0, q1-size: 0
    (test-equal? "get-min-cp-q-idx 1" (get-min-cp-q-idx (get-computepool env)) 0 pr wait)
    (test-equal? "get-max-cp-q-idx 1" (get-max-cp-q-idx (get-computepool env)) 0 pr wait)
    (test-equal? "q-len idx 0" (queue-length (get-cp-queue (get-computepool env) 0)) 0 pr wait)
    (test-equal? "q-len idx 1" (queue-length (get-cp-queue (get-computepool env) 1)) 0 pr wait)

    (enqueue! (get-cp-queue (get-computepool env) 0) (list (test-task) #f #f))
    (print-queue-lens env)

    ;q0-size: 1, q1-size: 0
    (test-equal? "get-min-cp-q-idx 2" (get-min-cp-q-idx (get-computepool env)) 1 pr wait)
    (test-equal? "get-max-cp-q-idx 2" (get-max-cp-q-idx (get-computepool env)) 0 pr wait)
    (test-equal? "q-len idx 0" (queue-length (get-cp-queue (get-computepool env) 0)) 1 pr wait)
    (test-equal? "q-len idx 1" (queue-length (get-cp-queue (get-computepool env) 1)) 0 pr wait)

    (enqueue! (get-cp-queue (get-computepool env) 1) (list (test-task) #f #f))
    (print-queue-lens env)

    ;q0-size: 1, q1-size: 1
    (test-equal? "get-min-cp-q-idx 3" (get-min-cp-q-idx (get-computepool env)) 0 pr wait)
    (test-equal? "get-max-cp-q-idx 3" (get-max-cp-q-idx (get-computepool env)) 0 pr wait)
    (test-equal? "q-len idx 0" (queue-length (get-cp-queue (get-computepool env) 0)) 1 pr wait)
    (test-equal? "q-len idx 1" (queue-length (get-cp-queue (get-computepool env) 1)) 1 pr wait)

    (enqueue! (get-cp-queue (get-computepool env) 1) (list (test-task) #f #f))

    ;q0-size: 1, q1-size: 2
    (test-equal? "get-min-cp-q-idx 4" (get-min-cp-q-idx (get-computepool env)) 0 pr wait)
    (test-equal? "get-max-cp-q-idx 4" (get-max-cp-q-idx (get-computepool env)) 1 pr wait)
    (test-equal? "q-len idx 0" (queue-length (get-cp-queue (get-computepool env) 0)) 1 pr wait)
    (test-equal? "q-len idx 1" (queue-length (get-cp-queue (get-computepool env) 1)) 2 pr wait)

    (close-dp env #t #t))
  (void))
;;**************************************


;;**************************************
;;TEST get-task-q-idx
;;     get-task
;;     dp-thread-exec-task
;;     dp-thread
;;     dp-thread-start
;;-------------------------------------- 
(: test-datapool-threads (-> Void))
(define (test-datapool-threads)
  (test-section "datapool thread internal functions")
  (let* ([num-threads 2]
         [cenv (computepool num-threads)]
         [env (datapool cenv)]) 
    (sleep 0.1)
    (: test-task-co (-> Any))
    (define-coroutine (test-task-co) 3)

    (: test-task (-> Any))
    (define test-task (test-task-co))

    (enqueue! (get-cp-queue cenv 0) (list test-task #f #f))

    (test-equal? "get-task-q-idx" (get-task-q-idx cenv 0) 0 pr wait)

    (dequeue! (get-cp-queue cenv 0))
    (enqueue! (get-cp-queue cenv 1) (list test-task #f #f))

    (test-equal? "get-task-q-idx 0" (get-task-q-idx cenv 0) 1 pr wait)
    (test-equal? "get-task-q-idx 1" (get-task-q-idx cenv 1) 1 pr wait)

    (enqueue! (get-cp-queue cenv 1) (list test-task #f #f))

    (test-equal? "get-task-q-idx 0" (get-task-q-idx cenv 0) 1 pr wait)
    (test-equal? "get-task-q-idx 1" (get-task-q-idx cenv 1) 1 pr wait)

    (enqueue! (get-cp-queue cenv 0) (list test-task #f #f))

    (test-equal? "get-task-q-idx 0" (get-task-q-idx cenv 0) 0 pr wait)
    (test-equal? "get-task-q-idx 1" (get-task-q-idx cenv 1) 1 pr wait)
    (let ([len-0 (queue-length (get-cp-queue cenv 0))]
          [task (get-task cenv 0)])
      (test-true? "get-task succeeds in pulling a task from the queue" 
                  (> len-0 (queue-length (get-cp-queue cenv 0))) 
                  pr 
                  wait))
    (close-dp env #t #t))
  (void))
;;**************************************


;;**************************************
;;TEST go
;;-------------------------------------- 
(: test-go (-> Void))
(define (test-go)
  (test-section "go")
  (let* ([num-threads 2]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])

    (let 
      ([inp-vals (list 'test 'test2 #f "teststring")]
       [ch (channel)]
       [inp-vals2 (list 'test3 'test4 #t "teststring2")])

      (: test-task-co2 (-> Channel Any Void))
      (define-coroutine 
        (test-task-co2 ch vals)
        (for ([val vals])
             (let ()
               (printf "Putting ~a in channel\n" val)
               (ch-put ch val))))

      (go env (test-task-co2 ch inp-vals))

      (sleep 0.1)
      (wait-len env)
      (for ([i num-threads])
           (printf "\n--- tests for thread ~a ---\n" i)
           (test-true? "Check if dp thread is not dead" (not (thread-dead? (get-cp-thread cenv 1))) pr wait)
           (test-true? "Check if dp thread is not running" (not (thread-running? (get-cp-thread cenv 1))) pr wait)
           (test-equal? "Verify task queue is empty" (queue-length (get-cp-queue cenv 0)) 0 pr wait))


      (printf "\n")
      (for ([i (length inp-vals)])
           (let ([val (list-ref inp-vals i)]
                 [ret (ch-get ch inp-vals)])
             (test-equal? "Did expected val get placed in channel" ret val pr wait)))


      (go env (test-task-co2 ch inp-vals2))
      (sleep 0.1)

      (for ([i num-threads])
           (printf "\n--- tests for thread ~a ---\n" i)
           (test-true? "Check if dp thread is dead" (not (thread-dead? (get-cp-thread cenv 1))) pr wait)
           (test-true? "Check if dp thread is actively running" (not (thread-running? (get-cp-thread cenv 1))) pr wait)
           (test-equal? "Verify task queue is empty" (queue-length (get-cp-queue cenv 0)) 0 pr wait))

      (printf "\n")
      (for ([i (length inp-vals2)])
           (let ([val (list-ref inp-vals2 i)]
                 [ret (ch-get ch #f)])
             (test-equal? "Did expected val get placed in channel" ret val pr wait))))
    (close-dp env #t #t))
  (void))
;;**************************************


;;**************************************
;;TEST hash-data!
;;     get-data  
;;     register-data!
;;-------------------------------------- 
(: test-data-hash (-> Void))
(define (test-data-hash)
  (test-section "manage data objects")
  (let* ([num-threads 2]
         [cenv (computepool num-threads)]
         [env (datapool cenv)]
         [test-key 1337]
         [test-key2 64]) 

    (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) 0 pr wait)
    (test-true? "hash-data! number" (hash-data! env test-key 3) pr wait)
    (test-true? "hash-data! string" (hash-data! env test-key "3") pr wait)
    (test-true? "hash-data! list" (hash-data! env test-key (list 3)) pr wait)
    (test-true? "hash-data! quote" (hash-data! env test-key '(3)) pr wait)

    (: test-class% Class)
    (define test-class%
      (class object% (super-new)
             (field [3-field 3])
             (define/public (get-3) 3-field)))

    (: test-object Object)
    (define test-object (make-object test-class%))

    (: test-val Number)
    (define test-val 1990)

    (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) 1 pr wait)
    (test-true? "hash-data! 1" (hash-data! env test-key test-object) pr wait)
    (test-true? "hash-data! 2" (hash-data! env test-key2 test-val) pr wait)

    (test-equal? "get-data 1" (send (get-data env test-key) get-3) 3 pr wait)
    (test-equal? "get-data 2" (get-data env test-key2) test-val pr wait) 

    (set-field! 3-field (get-data env test-key) 2)
    (test-equal? "set-field succeeded?" (get-field 3-field (get-data env test-key)) 2 pr wait) 


    (: test-class2% Class)
    (define test-class2%
      (class object% (super-new)
             (field [3-field 4])))

    (: test-object2 Object)
    (define test-object2 (make-object test-class2%))

    (let ([hash-size (hash-count (get-data-hash env))])
      (let ([key (register-data! env test-object2)])
        (test-equal? "register-data! object succeeds" key 0 pr wait)
        (test-equal? "get-data succeeds for test-object2" (get-data env key) test-object2 pr wait)
        (test-equal? "get-data-field succeeds for test-object2" (get-data-field env key '3-field) 4 pr wait)
        (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) (+ hash-size 1) pr wait)))
    (let ([hash-size (hash-count (get-data-hash env))])
      (let ([key (register-data! env test-object)])
        (test-equal? "register-data! object succeeds" key 1 pr wait)
        (test-equal? "get-data-hash hash-count" (hash-count (get-data-hash env)) (+ hash-size 1) pr wait)))
    (close-dp env #t #t))
  (void))
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
;;-------------------------------------- 
(: test-message-handlers (-> Void))
(define (test-message-handlers)
  (test-section "manage message handlers")
  (let* ([num-threads 2]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let*
      ([test-type 'test-type]
       [test-content "hello world"]
       [test-msg (message test-type test-content)])
      (test-equal? "can get message's type" 
                   (message-type test-msg) 
                   test-type 
                   pr 
                   wait)
      (test-equal? "can get message's content" 
                   (message-content test-msg) 
                   test-content 
                   pr 
                   wait)
      (test-equal? "can get message's source" 
                   (message-source test-msg) 
                   #f 
                   pr 
                   wait))


    (test-true? "get-dp-message-handler-hash" 
                (hash? (get-dp-message-handler-hash env)) 
                pr 
                wait)
    (test-true? "get-dp-message-handler-hash-sem" 
                (semaphore? (get-dp-message-handler-hash-sem env)) 
                pr 
                wait)

    (: test-type Symbol)
    (define test-type 'test-type)

    (: test-source Data-Key)
    (define test-source 12)

    (: test-source (-> Integer))
    (define callback-form (lambda () 1))
    (test-equal? "callback check" (callback-form) 1 pr wait)

    (let ([hash-ret (set-dp-message-handlers! 
                      env 
                      test-type 
                      test-source 
                      (list (list callback-form test-source #f)))])
      (test-true? "set-dp-message-handlers!" hash-ret pr wait))


    (let ([hash-ret (hash-ref (get-dp-message-handler-hash env) test-type #f)])
      (test-true? "returned value is a hash" (hash? hash-ret) pr wait)
      (test-equal? "(and set-dp-message-handlers! get-dp-message-handler-hash) 1" 
                   ((car (car (hash-ref hash-ret test-source #f))))
                   (callback-form) 
                   pr 
                   wait))

    (: test-source (-> Integer))
    (define callback-form-2 (lambda () 2))
    (test-equal? "callback check 2" (callback-form-2) 2 pr wait)

    (set-dp-message-handlers! 
      env 
      test-type 
      test-source
      (append 
        (hash-ref 
          (hash-ref (get-dp-message-handler-hash env) test-type) 
          test-source)
        (list (list callback-form-2 test-source #f))))

    (test-equal? "(and set-dp-message-handlers! get-dp-message-handler-hash) 2" 
                 (hash-ref 
                   (hash-ref (get-dp-message-handler-hash env) test-type) 
                   test-source)
                 (list 
                   (list callback-form test-source #f) 
                   (list callback-form-2 test-source #f))
                 pr 
                 wait)
    (let ([pre-handlers (list 
                          (list callback-form test-source #f) 
                          (list callback-form-2 test-source #f))]
          [post-handlers 
            (hash-ref 
              (hash-ref (get-dp-message-handler-hash env) test-type) 
              test-source)])
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
    (close-dp env #t #t))
  (void))


;;**************************************
;;TEST register-message-handler 
;;     send-message-co
;;     send-message 
;;     delete-data!
;;-------------------------------------- 
(: test-message-handlers-2 (-> Void))
(define (test-message-handlers-2)
  (test-section "manage message handlers 2")
  (let* ([num-threads 2]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])

    (: test-class% Class)
    (define test-class%
      (class object% 
             (super-new)
             (field [test-field 3])
             (field [test-field2 2])))

    (: test-object Object)
    (define test-object (make-object test-class%))

    (let ([hash-size (hash-count (get-data-hash env))])
      (let ([test-key (register-data! env test-object)])
        (test-equal? "register-data! object succeeds" test-key 0 pr wait)
        (test-equal? 
          "get-data-hash hash-count" 
          (hash-count (get-data-hash env)) 
          (+ hash-size 1) 
          pr 
          wait)

        ;Test various coroutine return-destination strategies work 
        (: msg-handler (-> Message (Listof Any)))
        (define-coroutine
          (msg-handler msg)
          (let*
            ([cont (message-content msg)]
             [env (car cont)]
             [ch (car (cdr cont))]
             [val (car (cddr cont))]
             [data-test-key (car (cdddr cont))])
            (ch-put ch val)
            (set-data-field! env data-test-key 'test-field (+ val 1))
            (list (+ val 2) 7)))

        (let
          ([test-msg-type 'test-msg-type]
           [test-msg-type-2 'test-msg-type-2]
           [test-source-key 3]
           [test-source-key-2 8]
           [ch (channel)]
           [test-val 4])

          (: catch-msg (-> Message Void))
          (define-coroutine 
            (catch-msg msg)
            (test-equal? "destination message type correct" 
                         (message-type msg)
                         test-msg-type-2 
                         pr
                         wait)
            (test-equal? "destination message source correct" 
                         (message-source msg)
                         test-source-key-2
                         pr 
                         wait)
            (test-equal? "destination message content correct" 
                         (message-content msg)
                         7
                         pr 
                         wait)
            (void))

          (register-message-handler 
            env 
            catch-msg 
            test-msg-type-2
            test-source-key-2
            #f 
            #f)

          (register-message-handler 
            env 
            msg-handler 
            test-msg-type 
            test-source-key
            #f
            (list (list '#:data test-key 'test-field2)
                  (list '#:message test-msg-type-2 test-source-key-2)))

          (test-equal? 
            "get-dp-message-handler-hash hash-count" 
            (hash-count (get-dp-message-handler-hash env)) 
            2
            pr
            wait)

          (send-message 
            env 
            (message test-msg-type (list env ch test-val test-key) test-source-key))

          (sleep 0.1)

          (test-equal? "ch has expected value" (ch-get ch) test-val pr wait)

          (test-equal? 
            "data test-field has expected value" 
            (get-data-field env test-key 'test-field)
            (+ test-val 1)
            pr
            wait)

          (test-equal? 
            "data test-field2 has expected value" 
            (get-data-field env test-key 'test-field2)
            (+ test-val 2)
            pr
            wait))

        (test-true? "delete-data!" (delete-data! env test-key) pr wait)
        (test-equal? "get-data-hash hash-count" 
                     (hash-count (get-data-hash env)) 
                     hash-size 
                     pr 
                     wait)

        (test-equal? 
          "get-dp-message-handler-hash hash-count" 
          (hash-count (get-dp-message-handler-hash env)) 
          1
          pr
          wait)
        (test-equal? "get-data fails" (get-data env test-key) 'not-found pr wait)



        ;test input-data field correctly passes input data to message handler
        (let ([out-ch (channel)]
              [test-msg-type 'test-type]) 

          (set! test-object (make-object test-class%))
          (set! test-key (register-data! env test-object))

          (: msg-handler-2 (-> Message (Listof Any)))
          (define-coroutine
            (msg-handler-2 msg input-data)
            (list (car input-data) (cadr input-data) (caddr input-data)))

          (register-message-handler 
            env 
            msg-handler-2
            test-msg-type 
            #f
            (list 
              (list test-key 'test-field2)
              (list test-key 'test-field)
              (list test-key #f))
            (list 
              (list '#:channel out-ch)
              (list '#:channel out-ch)
              (list '#:channel out-ch)))

          (send-message env (message test-msg-type #f))

          (sleep 0.1)

          (let ([ret1 (ch-get out-ch)])
            (test-true? "channel content exists"
                        (not (equal? ret1 'not-found))
                        pr 
                        wait)

            (test-equal? "channel content equals test-field2" 
                         (get-data-field env test-key 'test-field2)
                         ret1
                         pr 
                         wait))

          (let ([ret2 (ch-get out-ch)])
            (test-true? "channel content exists"
                        (not (equal? ret2 'not-found))
                        pr 
                        wait)
            (test-equal? "channel content equals test-field" 
                         (get-data-field env test-key 'test-field)
                         ret2
                         pr 
                         wait))

          (let ([ret3 (ch-get out-ch)])
            (test-true? "channel content exists"
                        (not (equal? ret3 'not-found))
                        pr 
                        wait)
            (test-equal? "channel content equals test-object" 
                         (get-data env test-key)
                         ret3
                         pr 
                         wait)))))
    (close-dp env #t #t))
  (void))
;;**************************************


;;**************************************
;;TEST mange message handlers 3: delete message handlers with deleted key set to source
;;-------------------------------------- 
(: test-message-handlers-3 (-> Void))
(define (test-message-handlers-3)
  (test-section "manage message handlers 3: delete message handlers with deleted key set to source")

  ;setup test data
  (define test-class%
    (class object% 
           (super-new)))

  (define test-object (make-object test-class%))

  (define test-message-type 'test-message-type)

  (: test-handler (-> Message Void))
  (define-coroutine 
    (test-handler msg)
    (print "test handler arg: ~a\n" (message-content msg))
    (void))


  (printf "1. delete-data! data-key where message handler has source set to data-key removes handler from handler hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              test-data-key 
                              #f
                              #f)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env)) 
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait))
    (close-dp env #t #t))

  (printf "2. delete-data! data-key where message handlers have source set to data-key removes handlers from handler hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (for ([i 3])
           (register-message-handler env 
                                   test-handler 
                                   test-message-type
                                   test-data-key 
                                   #f
                                   #f))

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait))
    (close-dp env #t #t))

  (printf "3. delete-data! data-key where message handler does *not* have source set to data-key remains in handler hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              (+ test-data-key 1)
                              #f
                              #f)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait))
    (close-dp env #t #t))

  (printf "4. delete-data! data-key where message handlers do *not* have source set to data-key remain in handler hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (for ([i 3])
           (register-message-handler env 
                                   test-handler 
                                   test-message-type
                                   (+ test-data-key 1)
                                   #f
                                   #f))

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait))
    (close-dp env #t #t))

  (printf "5. delete-data! data-key where 1 handler has source set to data-key and another handler does not, only deletes the first handler\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              test-data-key
                              #f
                              #f)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              (+ test-data-key 1)
                              #f
                              #f)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait))

    (close-dp env #t #t))
  (void))
;;**************************************


;;**************************************
;;TEST mange message handlers 4: delete message handlers with deleted key set to input-data
;;-------------------------------------- 
(: test-message-handlers-4 (-> Void))
(define (test-message-handlers-4)
  (test-section "manage message handlers 4: delete message handlers with deleted key set to input-data")

  ;setup test data
  (define test-class%
    (class object% 
           (super-new)))

  (define test-object (make-object test-class%))

  (define test-message-type 'test-message-type)

  (: test-handler (-> Message Void))
  (define-coroutine 
    (test-handler msg)
    (print "test handler arg: ~a\n" (message-content msg))
    (void))


  (printf "1. delete-data! data-key where message handler has input-data set to data-key deletes handler from hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              #f
                              (list (list '#:data test-data-key #f))
                              #f)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait))
    (close-dp env #t #t))

  (printf "2. delete-data! data-key where message handlers have input-data set to data-key deletes handlers from hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (for ([i 4])
           (register-message-handler env 
                                   test-handler 
                                   test-message-type
                                   #f
                                   (list (list '#:data test-data-key #f))
                                   #f))

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait))
    (close-dp env #t #t))

  (printf "3. delete-data! data-key where message handler does not have input-data to data-key does *not* delete handler from hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              #f
                              (list (list '#:data (+ test-data-key 1) #f))
                              #f)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait))
    (close-dp env #t #t))

  (printf "4. delete-data! data-key where message handlers do not have input-data set to data-key does *not* delete handlers from hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (for ([i 4])
           (register-message-handler env 
                                   test-handler 
                                   test-message-type
                                   #f
                                   (list (list '#:data (+ test-data-key 1) #f))
                                   #f))

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait))
    (close-dp env #t #t))

  (printf "5. delete-data! data-key where 1 handler has input-data set to data-key and another handler does not, only deletes the first handler\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              #f
                              (list (list '#:data test-data-key #f))
                              #f)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              #f
                              (list (list '#:data (+ test-data-key 1) #f))
                              #f)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait))
    (close-dp env #t #t))
  (void))
;;**************************************


;;**************************************
;;TEST mange message handlers 5: delete message handlers with deleted key set to return-destinations
;;-------------------------------------- 
(: test-message-handlers-5 (-> Void))
(define (test-message-handlers-5)
  (test-section "manage message handlers 5: delete message handlers with deleted key set to return-destinations")


  ;setup test data
  (define test-class%
    (class object% 
           (super-new)))

  (define test-object (make-object test-class%))

  (define test-message-type 'test-message-type)

  (: test-handler (-> Message Void))
  (define-coroutine 
    (test-handler msg)
    (print "test handler arg: ~a\n" (message-content msg))
    (void))


  (printf "1. delete data-key where message handler has return-destinations set to data-key deletes handler from hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              #f
                              #f
                              (list (list '#:data test-data-key #f)))

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait))
    (close-dp env #t #t))

  (printf "2. delete data-key where message handlers have return-destinations set to data-key deletes handlers from hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (for ([i 4])
           (register-message-handler env 
                                   test-handler 
                                   test-message-type
                                   #f
                                   #f
                                   (list (list '#:data test-data-key #f))))

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait))
    (close-dp env #t #t))

  (printf "3. delete data-key where message handler does not have return-destinations to data-key does *not* delete handler from hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              #f
                              #f
                              (list (list '#:data (+ test-data-key 1) #f)))

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait))
    (close-dp env #t #t))

  (printf "4. delete data-key where message handler does not have return-destinations to data-key does *not* delete handler from hash\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (for ([i 4])
           (register-message-handler env 
                                   test-handler 
                                   test-message-type
                                   #f
                                   #f
                                   (list (list '#:data (+ test-data-key 1) #f))))

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait))
    (close-dp env #t #t))

  (printf "5. delete data-key where 1 handler has return-destinations set to data-key and another handler does not, only deletes the first handler\n")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])
    (let ([test-data-key (register-data! env test-object)])
      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        0
        pr
        wait)

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              #f
                              #f
                              (list (list '#:data test-data-key #f)))

      (register-message-handler env 
                              test-handler 
                              test-message-type
                              #f
                              #f
                              (list (list '#:data (+ test-data-key 1) #f)))

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait)

      (test-true? "delete-data!" (delete-data! env test-data-key) pr wait)

      (test-equal? 
        "message handler size" 
        (hash-count (get-dp-message-handler-hash env))
        1
        pr
        wait))
    (close-dp env #t #t))
  (void))
;;**************************************


;;**************************************
;;TEST go ;stress test
;;-------------------------------------- 
(: test-go-stress (-> Void))
(define (test-go-stress)
  (test-section "go stress test: basic (go) invocations")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])

    (: test-routine (-> Integer True))
    (define-coroutine
      (test-routine inp-x)

      (: inner (-> Integer True))
      (define (inner x)
        (if (equal? x 0)
            #t
            (let ([x (- x 1)])
              (inner x))))
      (inner inp-x))


    (let ([start-time (current-inexact-milliseconds)]
          [v 100000])
      (for ([i v])
           (when (and (equal? (remainder i 10000) 0) (not (equal? i 0)))
             (printf "\tenqueued (go) i: ~a\n" i))
           (go env (test-routine v)))

      (wait-len env)
      (for ([i num-threads])
           (let ([o (open-output-string)])
             (fprintf o "length q[~a]" i)
             (test-equal? (get-output-string o) (queue-length (get-cp-queue cenv i)) 0 pr wait)))
      (let ([time (- (current-inexact-milliseconds) start-time)])
        (printf "Benchmark time (milli) for ~a (go) calls each iterating ~a times on ~a threads with no yields or returns\n" v v num-threads)
        (printf "loop iterations per second: ~a\n" (iterations-per-second time (* v v)))))

    (close-dp env #t #t))
  (void))
;;--------------------------------------


;;**************************************
;;TEST go ;stress test 2
;;-------------------------------------- 
(: test-go-stress-2 (-> Void))
(define (test-go-stress-2)
  (test-section "go stress test 2: timing comparisons for addition")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)])


    ;return immediately 
    (: define-coroutine (-> Any Any))
    (define-coroutine (go-return x) x)

    ;evaluate x times, yielding each time 
    (: eval-x-times-yield (-> Datapool Integer True))
    (define-coroutine 
      (eval-x-times-yield env inp-target)

      (: in-loop (-> Integer Integer True))
      (define (in-loop x target)
        (if (equal? x target)
            #t
            (let ([new-x (+ x 1)])
              (when (equal? (remainder x 10000) 0) (yield x))
              (in-loop new-x target))))
      (in-loop 0 inp-target))

    ;evaluate x times
    (: eval-x-times-yield (-> Datapool Integer True))
    (define-coroutine 
      (eval-x-times env inp-target)

      (: in-loop (-> Integer Integer True))
      (define (in-loop x target)
        (if (equal? x target)
            #t
            (let ([new-x (+ x 1)])
              (in-loop new-x target))))
      (in-loop 0 inp-target))

    ;parallel processing with futures
    (: eval-x-times-yield (-> Datapool Integer True))
    (define-coroutine
      (eval-x-times-parallel env inp-target)

      (: in-loop (-> Integer Integer True))
      (define (in-loop x target)
        (if (equal? x target)
            #t
            (let ([new-x (+ x 1)])
              (in-loop new-x target))))
      (future (thunk (in-loop 0 inp-target))))


    ;------------------------------------------------------------------------
    (sleep 1.0)
    (let ([start-time (current-inexact-milliseconds)]
          [x 100000])

      (for ([i x])
           (go env (go-return i)))

      (wait-len env)
      (let ([time (- (current-inexact-milliseconds) start-time)])
        (printf "Benchmark time (milli) for ~a immediately returning (go) operations: ~a\n" x time)
        (printf "(go) operations per second: ~a\n\n" (iterations-per-second time x))))


    (let ([x 1000000]) ;1,000,000 


      ;------------------------------------------------------------------------ 
      ;Test eval-x-times with yields
      (sleep 1.0)
      (let ([start-time (current-inexact-milliseconds)]
            [iterations (* num-threads x)])

        (for ([u num-threads])
             (go env (eval-x-times-yield env x)))

        (wait-len env)
        (let ([time (- (current-inexact-milliseconds) start-time)])
          (printf "Benchmark time (milli) for ~a (go) calls with ~a evaluations on ~a threads with (yield) calls every 10000 iterations: ~a\n"  num-threads x num-threads time)
          (printf "loop iterations per second: ~a\n\n" (iterations-per-second time iterations))))


      ;------------------------------------------------------------------------ 
      ;Test eval-x-times without yields
      (sleep 1.0)
      (let ([start-time (current-inexact-milliseconds)]
            [iterations (* num-threads x)])
        (for ([i num-threads])
             (go env (eval-x-times env x)))

        (wait-len env)
        (let ([time (- (current-inexact-milliseconds) start-time)])
          (printf "Benchmark time (milli) for ~a (go) calls with ~a evaluations on ~a threads in coroutine without (yield) calls: ~a\n"  num-threads x num-threads time)
          (printf "loop iterations per second: ~a\n\n" (iterations-per-second time iterations))))


      ;------------------------------------------------------------------------ 
      ;Test eval-x-times-parallel
      (sleep 0.5)
      (let ([start-time (current-inexact-milliseconds)]
            [iterations (* num-threads x)])

        (for ([i num-threads])
             (go env (eval-x-times-parallel env x)))

        (wait-len env)
        (let ([time (- (current-inexact-milliseconds) start-time)])
          (printf "Benchmark time (milli) for ~a (go) calls with ~a evaluations on ~a threads and ~a parallel processed futures: ~a\n"  num-threads x num-threads 8 time)
          (printf "loop iterations per second: ~a\n\n" (iterations-per-second time iterations))))
      (close-dp env #t #t)))
  (void))
;;-------------------------------------- 


;;**************************************
;;TEST go stress test 3: collating results
;;-------------------------------------- 
(: test-go-stress-3 (-> Void))
(define (test-go-stress-3)
  (test-section "go stress test 3: collating results")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)]
         [x 100000]
         [ch (channel)])

    ;------------------------------------------------------------------------ 
    ;collate results in a channel 
    (: collate-coroutine (-> Channel Any Void))
    (define-coroutine
      (collate-coroutine ch val)
      (ch-put ch val))

    (let ([start-time (current-inexact-milliseconds)])
      (for ([i x]) 
           (let ()
             (when (equal? 0 (remainder i 10000))
               (printf "enqueued (go)[~a]\n" i))
             (go env (collate-coroutine ch i))))
      (wait-len env)
      (let ([time (- (current-inexact-milliseconds) start-time)])
        (printf "Benchmark time (milli) for ~a (go) calls with ~a evaluations on ~a threads collating results in a shared channel: ~a\n"  x 1 num-threads time)
        (printf "go invocations per second: ~a\n\n" (iterations-per-second time x))))

    ;------------------------------------------------------------------------ 
    ;write results in the data hash 
    (: collate-class% Class)
    (define collate-class%
      (class object% 
             (super-new)
             (field [val (channel)])))

    (: collate-object Object)
    (define collate-object (make-object collate-class%))

    (: ret-val-coroutine (-> Any Any))
    (define-coroutine
      (ret-val-coroutine val)
      val)

    (let ([obj-key (register-data! env collate-object)]
          [start-time (current-inexact-milliseconds)])
      (for ([i x]) 
           (let ()
             (when (equal? 0 (remainder i 10000))
               (printf "enqueue data collate (go)[~a]\n" i))
             (go 
               env 
               (ret-val-coroutine i) 
               (list 
                 (list '#:channel (get-data-field env obj-key 'val))))))
      (wait-len env)
      (let ([time (- (current-inexact-milliseconds) start-time)])
        (printf "Benchmark time (milli) for ~a (go) calls with ~a evaluations on ~a threads collating results in a shared hashed object's field: ~a\n"  x 1 num-threads time))

      (test-true? 
        "val stored in shared data object" 
        (not (equal? (get-data-field env obj-key 'val) 'not-found))
        pr 
        wait)
      (printf "Val stored in shared data object: ~a\n" (get-data-field env obj-key 'val)))

    ;------------------------------------------------------------------------ 
    ;handle results in the data hash using message handler

    (close-dp env #t #t))
  (void))
;;--------------------------------------


;;**************************************
;;TEST test multiple datapools
;;-------------------------------------- 
(: test-multiple-datapool (-> Void))
(define (test-multiple-datapool)
  (test-section "test multiple datapools")
  (let* ([num-threads 8]
         [cenv (computepool num-threads)]
         [env (datapool cenv)]
         [env2 (datapool cenv)]
         [ch (channel)])

    (: test-val Integer)
    (define test-val 17)

    (: test-val-2 Integer)
    (define test-val-2 2)

    (: test-val-3 Integer)
    (define test-val-3 24)

    (: test-val-4 Integer)
    (define test-val-4 55)

    (: output-to-other-datapool (-> Any Any))
    (define-coroutine
      (output-to-other-datapool val)
      val)

    (: test-env2-class% Class)
    (define test-env2-class%
      (class object% 
             (super-new)
             (field [test-field test-val-2])))

    (: test-env2-object Object)
    (define test-env2-object (make-object test-env2-class%))

    (let ([test-key (register-data! env2 test-env2-object)])
      (go env 
          (output-to-other-datapool test-val) 
          (list (list '#:datapool env2 test-key 'test-field)))

      (wait-len env)

      (test-equal? "successfully set env2 field from env" 
                   (get-data-field env2 test-key 'test-field) 
                   test-val
                   pr 
                   wait)
      (go env 
          (output-to-other-datapool test-val-3) 
          (list (list '#:datapool env2 test-key 'test-field)))

      (wait-len env)

      (test-equal? "successfully set env2 field from env" 
                   (get-data-field env2 test-key 'test-field) 
                   test-val-3
                   pr 
                   wait)

      (go env 
          (output-to-other-datapool test-val-4) 
          (list (list '#:datapool env2 test-key #f)))

      (wait-len env)

      (test-equal? "successfully set env2 data from env" 
                   (get-data env2 test-key) 
                   test-val-4
                   pr 
                   wait))

    (close-dp env #t #t)
    (close-dp env2 #t #t))
  (void))



;; PUBLIC API
(: run-3-unit-tests (-> Void))
(define 
  (run-3-unit-tests)
  (reset-test-results)

  (test-test-functions)
  (test-coroutines)
  (test-channels)
  (test-datapool-intern)
  (test-datapool-getters-setters)
  (test-task-queues)
  (test-datapool-threads)
  (test-go)
  (test-data-hash)
  (test-message-handlers)
  (test-message-handlers-2)
  (test-message-handlers-3)
  (test-message-handlers-4)
  (test-message-handlers-5)
  (test-go-stress)
  (test-go-stress-2)
  (test-go-stress-3)
  (test-multiple-datapool)

  (print-test-report)
  (void))

(run-3-unit-tests)
