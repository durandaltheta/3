#! /usr/bin/scheme --script
(let ()
  (import (chezscheme) (parallel) (test))

  (define pr #f)
  (define w #f)

  ;****************************************************************************
  (define (ut-task?)
    (let ([test-task (make-task (lambda () 3))]
          [test-thunk (lambda () 3)])
      (test-section "task?")
      (test-true? "Make sure our task is a task record" (task? test-task) pr w)
      (test-true? "Make sure our thunk is not a task" (not (task? test-thunk)) pr w)))

  ;****************************************************************************
  (define (ut-task-box?)
    (let* ([task-list (list (make-task (lambda () 3))
                            (make-task (lambda () (+ 3 2))))]
           [tb (make-task-box task-list)])
      (test-section "task-box?")
      (test-true? "did we make a task box?" (task-box? tb) pr w)))

  ;****************************************************************************
  (define (ut-enqueue-task!)
    (let* ([task-list (list (make-task (lambda () 1))
                            (make-task (lambda () 2)))]
           [tb (make-task-box task-list)]
           [new-task (make-task (lambda () 3))])
      (test-section "enqueue-task!")
      (with-exception-handler
        (lambda (x) (test-fail "enqueue errored" pr w))
        (let ()
          (enqueue-task! tb new-task)
          (test-pass "enqueue did not error" pr w))))
      (with-exception-handler
        (lambda (x) (test-pass "enqueue errored as expected" pr w))
        (let ()
          (enqueue-task! tb (lambda () 3))
          (test-pass "enqueue did not error incorrectly" pr w))))

  ;****************************************************************************
  (define (ut-parallel)
    (let* ([tb (make-task-box (list (make-task (lambda () (* 3 7)))
                                    (make-task (lambda () (/ 400 8)))))]
           [res (parallel tb)])
      (test-section "parallel")
      (test-true? "result list not null" (not (null? res)) pr w)
      (test-equal? "expected result 1" (car res) 21 pr w)
      (test-true? "next entry in result list is not null" (not (null? (cdr res))) pr w)
      (test-equal? "expected result 2" (cadr res) 50 pr w)))

  ;****************************************************************************
  (define (ut-start-go)
    (test-section "start-go")

    (go (lambda () (* 35 40)))
    (go (lambda () (- 5 2)))
    (let ([res (start)])
      (test-equal? "expected result 1" (car res) 1400 pr w)
      (test-equal? "expected result 2" (cadr res) 3 pr w))

    (go (lambda () (go (lambda () 3)) 1))
    (go (lambda () 2))
    (let ([res (start)])
      (test-equal? "expected result 3" (car res) 1 pr w)
      (test-equal? "expected result 4" (cadr res) 2 pr w)
      (test-equal? "expected result 5" (caddr res) 3 pr w)))

  ;****************************************************************************
  (define (ut-parallel-channel)
    (test-section "parallel-channel")

    (let ([ch (make-parallel-channel)])
      (test-true? "did we make a parallel-channel?" (parallel-channel? ch) pr w)

      (ch-put! ch 'test)

      (test-equal? "did our put! and get! succeed?" 'test (ch-get! ch) pr w)

      (ch-put! ch 3)
      (ch-put! ch "test")

      (test-true? "make sure the channel is not empty" (not (ch-empty? ch)) pr w)
      (test-equal? "did our 2nd put! and get! succeed?" 3 (ch-get! ch) pr w)
      (test-equal? "did our 3rd put! and get! succeed?" "test" (ch-get! ch) pr w)
      (test-true? "make sure the channel is empty" (ch-empty? ch) pr w))

    (let ([ch1 (make-parallel-channel)]
          [ch2 (make-parallel-channel)])
      
      (define (catch-pass who in-ch out-ch count limit)
                         (let ([ball (ch-get! in-ch)])
                           (if (< count limit)
                             (let ()
                               (printf "~a passes the ball!\n" who)
                               (ch-put! out-ch ball)
                               (catch-pass who (+ count 1) limit))
                             who)))

      (go (lambda () (catch-pass "thunk1" ch1 ch2 0 10)))
      (go (lambda () (catch-pass "thunk2" ch2 ch1 0 10)))
      (ch-put! ch1 'ball)
      (printf "\n")

      (let ([res (start)])
        (printf "\n")
        (test-equal? "should be thunk1" "thunk1" (car res) pr w)
        (test-equal? "should be thunk2" "thunk2" (cadr res) pr w))))

  #|
  practical-channel-parallel
  practical-channel-start-go
  practical-use-case
  |# 
  
  ;****************************************************************************
  (define (run-ut-parallel print-result wait)
    (set! pr print-result)
    (set! w wait)
    (ut-task?)
    (ut-task-box?)
    (ut-parallel)
    (ut-start-go)
    (ut-parallel-channel)
    (print-test-report))

  (run-ut-parallel #t #f))
