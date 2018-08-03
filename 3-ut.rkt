#lang racket
(require 3)

;;;---------------------------------------------------------------------------- 
;;; Function Tests
;;;---------------------------------------------------------------------------- 
;;**************************************
;;TEST test-true? test-equal?
(test-true? #t)
(test-true? (not #t))
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
           (car (vector-ref (vector-ref (get-dp-data) 0) i)))))

  ;Verify the threads are alive
  (for ([i num-threads])
       (test-true? (not (thread-dead? 
                          (car 
                            (vector-ref 
                              (vector-ref (get-dp-data) 0) 
                              i))))))

  ;Verify task queues exist
  (for ([i num-threads])
       (test-true? 
         (queue? 
           (cdr (vector-ref (vector-ref (get-dp-data) 0) i)))))

  ;Verify task queue semaphores exist
  (for ([i num-threads])
       (test-true? 
         (semaphore? 
           (cddr (vector-ref (vector-ref (get-dp-data) 0) i)))))

  ;Verify hash table of message handlers exists
  (test-true? 
    (hash? 
      (car (vector-ref (get-dp-data) 1))))

  ;Verify hash table semaphore exists
  (test-true? 
    (semaphore? 
      (cdr (vector-ref (get-dp-data) 1))))

  ;Verify parent->dp channel exists
  (test-true? 
    (async-channel? 
      (car (vector-ref (get-dp-data) 2))))

  ;Verify dp->parent channel exists
  (test-true? 
    (async-channel? 
      (cdr (vector-ref (get-dp-data) 2))))

  ;Verify we can kill the datapool environment
  (close-dp *datapool-environment-data*)

  (for ([i num-threads])
       (test-true? (thread-dead? 
                     (car 
                       (vector-ref 
                         (vector-ref (get-dp-data) 0) 
                         i))))))
;;**************************************


;;**************************************
;;TEST get-dp-channel
(let ([num-threads 2]
      [*datapool-environment-data* (make-dp-data num-threads)])

  ;Check that parent->dp channel exists
  (test-true? (async-channel? (get-dp-channel *datapool-environment-data*)))
  (close-dp *datapool-environment-data*))
;;**************************************



;;;---------------------------------------------------------------------------- 
;;; Feature Tests
;;;---------------------------------------------------------------------------- 
;TODO figure out how to get argv & argc 
;(define dp1 (datapool 4 '(main argv argc)))
;(let ([ch (get-dp-channel dp1)]) 
;(<- ch))
