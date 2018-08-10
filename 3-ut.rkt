#lang racket

;;;---------------------------------------------------------------------------- 
;;; Function Tests
;;;---------------------------------------------------------------------------- 
;;**************************************
;;TEST test-true? test-equal?
(test-true? #t)
(test-true? (not #f))
(test-equal? 1 1)
(test-equal? "test" "test")
(test-equal? 'test 'test)
(test-equal? '("test" "test2") '("test" "test2"))
;;**************************************


;;**************************************
;;TEST define-coroutine 
;; Run tests for co-routines
(define-coroutine
  (co-test1) 
  (yield 1)
  (yield 2)
  #f)


;Test the coroutine runs correctly
(test-equal? (co-test1) 1)
(test-equal? (co-test1) 2)
(test-true? (not (co-test1)))

(define-coroutine
  (co-test2 arg1 arg2)
  (yield arg1)
  (yield arg2)
  #f)

;Test coroutine can be run with arguments
(test-equal? (co-test2 "teststring" 'test)  "teststring")
(test-equal? (co-test2)  'test)
(test-true? (not co-test2))

;Test coroutine can be rerun with new arguments
(test-equal? (co-test2 'test2 "teststring2")  'test2)
(test-equal? (co-test2)  "teststring2")
(test-true? (not co-test2))
;;**************************************


;;**************************************
;;TEST func
;; Tests for (func) definitions
(func
  (co-test1) 
  (yield 1)
  (yield 2)
  #f)

;Test the coroutine runs correctly
(test-equal? (co-test1) 1)
(test-equal? (co-test1) 2)
(test-true? (not (co-test1)))

(func
  (co-test2 arg1 arg2)
  (yield arg1)
  (yield arg2)
  #f)

;Test coroutine can be run with arguments
(test-equal? (co-test2 "teststring" 'test)  "teststring")
(test-equal? (co-test2)  'test)
(test-true? (not co-test2))

;Test coroutine can be rerun with new arguments
(test-equal? (co-test2 'test2 "teststring2")  'test2)
(test-equal? (co-test2)  "teststring2")
(test-true? (not co-test2))

;Should raise an exception due to the stateful (set!)
(with-handlers ([exn:fail:user? (lambda (e) (test-pass e))])
               (func 
                 (co-test3 arg1)
                 (yield arg1)
                 (set! arg1 2)
                 (yield arg2))
               (test-fail (co-test3 1)))
;;**************************************


;;**************************************
;;TEST danger-func!
;; Tests for (danger-func!) definitions
(danger-func!
  (co-test1) 
  (yield 1)
  (yield 2)
  #f)

;Test the coroutine runs correctly
(test-equal? (co-test1) 1)
(test-equal? (co-test1) 2)
(test-true? (not (co-test1)))

(danger-func!
  (co-test2 arg1 arg2)
  (yield arg1)
  (yield arg2)
  #f)

;Test coroutine can be run with arguments
(test-equal? (co-test2 "teststring" 'test)  "teststring")
(test-equal? (co-test2)  'test)
(test-true? (not co-test2))

;Test coroutine can be rerun with new arguments
(test-equal? (co-test2 'test2 "teststring2")  'test2)
(test-equal? (co-test2)  "teststring2")
(test-true? (not co-test2))

;Should *not* raise an exception due to the stateful (set!)
(with-handlers ([exn:fail:user? (lambda (e) (test-fail e))])
               (danger-func! 
                 (co-test3 arg1)
                 (yield arg1)
                 (set! arg1 2)
                 (yield arg2))
               (test-equal? (co-test3 1) 1)
               (test-equal? (co-test3) 2)
               (test-true? (not (co-test3))))
;;**************************************


;;**************************************
;;TEST channel
(let ([ch (channel)])
  (test-true? (async-channel? ch)))
;;**************************************


;;**************************************
;;TEST <-
(let ([ch (channel)])
  (async-channel-put ch "teststring")
  (let ([ret (<- ch #f)])
    (test-equal? ret "teststring")))

(let ([ch (channel)])
  (async-channel-put ch "teststring2")
  (let ([ret (<- ch)])
    (test-equal? ret "teststring2")))
;;**************************************


;;**************************************
;;TEST ->
(let ([ch (channel)])
  (-> ch "teststring")
  (let ([ret (<- ch #f)])
    (test-equal? ret "teststring")))

(let ([ch (channel)])
  (-> ch "teststring2")
  (let ([ret (<- ch)])
    (test-equal? ret "teststring2")))
;;**************************************


;;**************************************
;;TEST make-dp-data get-dp-data close-dp
;Sanity test
(test-equal? (get-dp-data) #f)

;Make a datapool
(let ([num-threads 2]
      [*datapool-environment-data* (make-dp-data num-threads)])

  ;Verify threads exist
  (for ([i num-threads])
       (test-true? 
         (thread? 
           (vector-ref (vector-ref (vector-ref (get-dp-data) 0) i) 0))))

  ;Verify the threads are alive
  (for ([i num-threads])
       (test-true? (not (thread-dead? 
                          (vector-ref
                            (vector-ref 
                              (vector-ref (get-dp-data) 0) 
                              i) 0)))))

  ;Verify task queues exist
  (for ([i num-threads])
       (test-true? 
         (queue? 
           (vector-ref (vector-ref (vector-ref (get-dp-data) 0) i) 1))))

  ;Verify task queue semaphores exist
  (for ([i num-threads])
       (test-true? 
         (semaphore? 
           (vector-ref (vector-ref (vector-ref (get-dp-data) 0) i) 2))))

  ;Verify hash table of message handlers exists
  (test-true? 
    (hash? 
      (vector-ref (vector-ref (get-dp-data) 1) 0)))

  ;Verify hash table semaphore exists
  (test-true? 
    (semaphore? 
      (vector-ref (vector-ref (get-dp-data) 1) 1)))

  ;Verify parent->dp channel exists
  (test-true? 
    (async-channel? 
      (vector-ref (vector-ref (get-dp-data) 2) 0)))

  ;Verify dp->parent channel exists
  (test-true? 
    (async-channel? 
      (vector-ref (vector-ref (get-dp-data) 2) 1)))

  ;Verify we can kill the datapool environment
  (close-dp *datapool-environment-data*)

  (for ([i num-threads])
       (test-true? (thread-dead? 
                     (vector-ref 
                       (vector-ref 
                         (vector-ref (get-dp-data) 0) i) 0)))))
;;**************************************


;;**************************************
;;TEST get-dp-channel get-num-dp-threads get-dp-thread get-dp-queue 
;;TEST get-dp-queue-sem get-dp-data-objects get-dp-data-objects-sem 
;;TEST get-dp-data-objects get-dp-data-objects-sem gen-dp-data-obj-key
(let ([num-threads 2]
      [*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
      [*datapool-environment-data* (make-dp-data num-threads)]
      [*data-obj-key-src* 0])

  ;Check that parent->dp channel exists
  (test-true? (async-channel? (get-dp-channel *datapool-environment-data*)))

  ;Check correct num of threads exist 
  (test-equal? (get-num-dp-threads) 2)
  
  ;Check threads exist 
  (test-true? (thread? (get-dp-thread 0)))
  (test-true? (thread? (get-dp-thread 1)))

  ;Check thread task queues exist
  (test-true? (queue? (get-dp-queue 0)))
  (test-true? (queue? (get-dp-queue 1)))

  ;Check thread task queues semaphores exist
  (test-true? (semaphore? (get-dp-queue-sem 0)))
  (test-true? (semaphore? (get-dp-queue-sem 1)))

  ;Check data object hash exists
  (test-true? (hash? (get-dp-data-objects)))
  (test-true? (semaphore? (get-dp-data-objects-sem)))

  ;Check key generation Function
  (test-equal? (gen-dp-data-obj-key) 0)
  (test-equal? (gen-dp-data-obj-key) 1)
  (test-equal? (gen-dp-data-obj-key) 2)
  (test-equal? (gen-dp-data-obj-key) 3)
  (close-dp *datapool-environment-data*))
;;**************************************

;;**************************************
;;TEST get-min-dp-q-idx get-max-dp-q-idx go
(let ([num-threads 2]
      [*dp-thread-continuous-eval-limit* *dp-thread-continuous-eval-limit*]
      [*datapool-environment-data* (make-dp-data num-threads)]
      [*data-obj-key-src* 0])

  ;;Arbitrary function to execute
  (define (test-task) #t)

  ;;Test defaults
  ;q0-size: 0, q1-size: 0
  (test-equal? (get-min-dp-q-idx) 0)
  (test-equal? (get-max-dp-q-idx) 0)
  (test-equal? (queue-length (get-dp-queue 0)) 0)
  (test-equal? (queue-length (get-dp-queue 1)) 0)

  (enqueue! (get-dp-queue 0) test-task)

  ;q0-size: 1, q1-size: 0
  (test-equal? (get-min-dp-q-idx) 1)
  (test-equal? (get-max-dp-q-idx) 0)
  (test-equal? (queue-length (get-dp-queue 0)) 1)
  (test-equal? (queue-length (get-dp-queue 1)) 0)

  (enqueue! (get-dp-queue 1) test-task)
  
  ;q0-size: 1, q1-size: 1
  (test-equal? (get-min-dp-q-idx) 0)
  (test-equal? (get-max-dp-q-idx) 0)
  (test-equal? (queue-length (get-dp-queue 0)) 1)
  (test-equal? (queue-length (get-dp-queue 1)) 1)

  (enqueue! (get-dp-queue 1) test-task)

  ;q0-size: 1, q1-size: 2
  (test-equal? (get-min-dp-q-idx) 0)
  (test-equal? (get-max-dp-q-idx) 1)
  (test-equal? (queue-length (get-dp-queue 0)) 1)
  (test-equal? (queue-length (get-dp-queue 1)) 2)

  ;resume thread execution
  (go test-task) 
  (sleep 1)

  ;threads should be completed and asleep again
  ;q0-size: 0, q1-size: 0
  (test-equal? (get-min-dp-q-idx) 0)
  (test-equal? (get-max-dp-q-idx) 0)
  (test-equal? (queue-length (get-dp-queue 0)) 0)
  (test-equal? (queue-length (get-dp-queue 1)) 0)

  (close-dp *datapool-environment-data*))
;;**************************************


;;;---------------------------------------------------------------------------- 
;;; Feature Tests
;;;---------------------------------------------------------------------------- 
;TODO figure out how to get argv & argc 
;(define dp1 (datapool 4 '(main argv argc)))
;(let ([ch (get-dp-channel dp1)]) 
;(<- ch))


;;;---------------------------------------------------------------------------- 
;;; Closing Analysis
;;;---------------------------------------------------------------------------- 
(exit)
