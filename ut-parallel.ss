#! /usr/bin/scheme --script
(let ()
  (import (chezscheme) (parallel) (test))

  (define pr #f)
  (define w #f)

  ;****************************************************************************
  (define (ut-parallel)
    (let ([tb (make-task-box)])
      (test-section "parallel")
      (test-true? "did we make a task box?" (task-box? tb) pr w)

      (go tb (lambda () (* 3 7)))
      (go tb (lambda () (/ 400 8)))
      (let ([res (parallel tb)])
        (test-true? "result list not null" (not (null? res)) pr w)
        (test-equal? "expected result 1" (car res) 21 pr w)
        (test-true? "next entry in result list is not null" (not (null? (cdr res))) pr w)
        (test-equal? "expected result 2" (cadr res) 50 pr w))

      (go tb (lambda () (* 35 40)))
      (go tb (lambda () (- 5 2)))
      (let ([res (parallel tb)])
        (test-equal? "expected result 1" (car res) 1400 pr w)
        (test-equal? "expected result 2" (cadr res) 3 pr w))

      (go tb (lambda () (go tb (lambda () 3)) 1))
      (go tb (lambda () 2))
      (let ([res (parallel tb)])
        (test-equal? "expected result 3" (car res) 1 pr w)
        (test-equal? "expected result 4" (cadr res) 2 pr w)
        (test-equal? "expected result 5" (caddr res) 3 pr w))))

  ;****************************************************************************
  (define (ut-parallel-channel)
    (test-section "parallel-channel")

    (let* ([tb (make-task-box)]
           [ch (make-parallel-channel tb)])
      (test-true? "did we make a parallel-channel?" (parallel-channel? ch) pr w)

      (ch-put! ch 'test)

      (test-equal? "did our put! and get! succeed?" 'test (ch-get! ch) pr w)

      (ch-put! ch 3)
      (ch-put! ch "test")

      (test-true? "make sure the channel is not empty" (not (ch-empty? ch)) pr w)
      (test-equal? "did our 2nd put! and get! succeed?" 3 (ch-get! ch) pr w)
      (test-equal? "did our 3rd put! and get! succeed?" "test" (ch-get! ch) pr w)
      (test-true? "make sure the channel is empty" (ch-empty? ch) pr w))

    (let ([tb (make-task-box)])
      (define (catch-pass who in-ch out-ch limit)
        (define (loop who in-ch out-ch count limit)
          (if (< count limit)
            ;catch the ball
            (let ([ball (ch-get! in-ch)])
              (set-unsafe! tb)
              (printf "~a catches the ~a\n" who ball)
              (set-safe! tb)
              ;pass the ball
              (ch-put! out-ch ball)
              (set-unsafe! tb)
              (printf "~a passes the ~a!\n" who ball)
              (set-safe! tb)
              (loop who in-ch out-ch (+ count 1) limit))
            (let ()
              (set-unsafe! tb)
              (printf "~a finished\n" who)
              (set-safe! tb)
              who)))
        ;set starting loop values
        (loop who in-ch out-ch 0 limit))

      (let ([ch1 (make-parallel-channel tb)]
            [ch2 (make-parallel-channel tb)]
            [pass-limit 3])
        (go tb (lambda () (catch-pass "thunk1" ch1 ch2 pass-limit)))
        (go tb (lambda () (catch-pass "thunk2" ch2 ch1 pass-limit)))
        (ch-put! ch1 'ball)
        (printf "\n"))

      (let ([res (parallel tb)])
        (printf "\n")
        (if (eqv? '() res)
          (test-fail "results are empty" pr w)
          (let ()
            (test-equal? "should be thunk1" "thunk1" (car res) pr w)
            (test-equal? "should be thunk2" "thunk2" (cadr res) pr w))))))

  ;****************************************************************************
  (define (run-ut-parallel print-result wait)
    (set! pr print-result)
    (set! w wait)
    (ut-parallel)
    (ut-parallel-channel)
    (print-test-report))

  (run-ut-parallel #t #f))
