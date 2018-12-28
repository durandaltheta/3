(library 
  (test)
  (export
    ;;;TESTING
    test-section
    test-true?
    test-equal?
    test-fail
    test-pass
    print-test-report
    reset-test-results)
  (import (chezscheme))


  ;;;----------------------------------------------------------------------------
  ;;; TESTING - functions
  ;;;---------------------------------------------------------------------------- 
  ;; To wait for user input on test failure 
  ;; (define *run-3-tests-wait-before-cont* #t) 

  ;Global test result tallies 
  (define *test-number* 1)
  (define *num-passes* 0)
  (define *passed-tests* '())
  (define *num-fails* 0)
  (define *failed-tests* '())
  (define *cur-test-section* "")
  (define *tests-started* #f)

  ;Print test divider
  (define (print-test-divider char)
    (if (string? char)
      (let ()
        (do ([i 0 (+ i 1)])
          ((>= i 80) '())
          (printf "~a" char))
        (printf "\n"))))

  ;Zero test result tallies
  (define (reset-test-results)
    (set! *test-number* 1)
    (set! *num-passes* 0)
    (set! *passed-tests* (list))
    (set! *num-fails* 0)
    (set! *failed-tests* (list))
    (set! *cur-test-section* "")
    (set! *tests-started* #f))

  (define (get-test-num)
    (let ([num *test-number*])
      (set! *test-number* (add1 *test-number*))
      num))

  ;Append test results to the global tallies
  (define (collate-test pass-fail)
    (if (car pass-fail)
      (let ([test-string (cdr pass-fail)])
        (set! *num-passes* (add1 *num-passes*))
        (set! *passed-tests* (append *passed-tests* (list test-string))))
      (let ([test-string (cdr pass-fail)])
        (set! *num-fails* (add1 *num-fails*))
        (set! *failed-tests* (append *failed-tests* (list test-string))))))

  ;; PUBLIC API
  ;Print final test results
  (define (print-test-report)
    (define (iter-tests ftests)
      (if (null? ftests)
        '()
        (let ()
          (printf "\t~a" (car ftests))
          (iter-tests (cdr ftests)))))
    (printf "\nNumber of test passes: ~a\n" *num-passes*)
    (printf "Number of test failures: ~a\n\n" *num-fails*)
    (printf "Failed tests:\n")
    (iter-tests *failed-tests*)
    (reset-test-results))

  ;; PUBLIC API
  ;Designate & print current test section with description
  (define (test-section name)
    (set! *cur-test-section* name)
    (if *tests-started*
      (let ()
        (print-test-report)
        (printf "\n\n"))
      (set! *tests-started* #t))
    (print-test-divider "#")
    (printf "### test-section ~a\n" name)
    (print-test-divider "#"))

  ;; PUBLIC API
  ;; Return #t if the quoted form returns #t, else #f 
  (define test-true?
    (let ()
      (define (run description form print-result wait)
        (if (equal? form #t)
          (let ([o (open-output-string)])
            (fprintf o "pass TEST ~a ~a: #t\n" (get-test-num) description)
            (when print-result
              (printf (get-output-string o)))
            (let ([ret (cons #t (get-output-string o))])
              (collate-test ret)
              ret))
          (let ([o (open-output-string)])
            (fprintf o "FAIL TEST ~a ~a: ~a != #t\n" (get-test-num) description form)
            (when print-result
              (printf (get-output-string o))
              (when wait
                (let ()
                  (printf "<enter to continue>")
                  (read))))
            (let ([ret (cons #f (get-output-string o))])
              (collate-test ret)
              ret))))
      (case-lambda 
        [(desc form) (run desc form #t #f)]
        [(desc form print-result) (run desc form print-result #f)]
        [(desc form print-result wait) (run desc form print-result wait)])))

  ;; PUBLIC API
  ;; Return #t if quoted forms return an equal value, else #f
  (define test-equal?
    (let ()
      (define (run description form-a form-b print-result wait)
        (if (equal? form-a form-b)
          (let ([o (open-output-string)])
            (fprintf o "pass TEST ~a ~a: ~a == ~a\n" (get-test-num) description form-a form-b)
            (when print-result
              (printf (get-output-string o)))
            (let ([ret (cons #t (get-output-string o))])
              (collate-test ret)
              ret))
          (let ([o (open-output-string)])
            (fprintf o "FAIL TEST ~a ~a: ~a != ~a\n" (get-test-num) description form-a form-b)
            (when print-result
              (printf (get-output-string o))
              (when wait
                (let ()
                  (printf "<enter to continue>")
                  (read))))
            (let ([ret (cons #f (get-output-string o))])
              (collate-test ret)
              ret))))
      (case-lambda 
        [(description a b) (run description a b #f #f)]
        [(description a b print-result) (run description a b print-result #f)]
        [(description a b print-result wait) (run description a b print-result wait)])))


  ;; PUBLIC API
  ;; Custom test fail
  (define test-fail
    (let ()
      (define (run description form print-result wait)
        (let ([o (open-output-string)])
          (fprintf o "FAIL TEST ~a ~a: ~a\n" (get-test-num) description form)
          (when print-result
            (printf (get-output-string o))
            (when wait
              (let ()
                (printf "<enter to continue>")
                (read))))
          (let ([ret (cons #f (get-output-string o))])
            (collate-test ret)
            ret)))
      (case-lambda 
        [(description form) (run description form #f #f)]
        [(description form print-result) (run description form print-result #f)]
        [(description form print-result wait) (run description form print-result wait)])))


  ;; PUBLIC API
  ;; Custom test pass
  (define test-pass
    (let ()
      (define (run description form print-result)
        (let ([o (open-output-string)])
          (fprintf o "pass TEST ~a ~a: ~a\n" (get-test-num) description form)
          (when print-result
            (printf (get-output-string o)))
          (let ([ret (cons #t (get-output-string o))])
            (collate-test ret)
            ret)))
      (case-lambda
        [(description form) (run description form #f)]
        [(description form print-result) (run description form print-result)]))))
