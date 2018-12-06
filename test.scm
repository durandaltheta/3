
;;;;Data wants to be structured and stateful 
;;;;Functions want to be functional and stateless
;;;;Computation wants to be fast, efficient, asynchronous and seamless


;;;TODO:
;;; 1. Put everything in structs to make internal implementation less brittle
;;; 2. Improve (go) efficiency 
;;; 3. Implement and improve error handling
;;; 4. Implement input argument santization
;;; 5. Possibly improve hash and/or queue efficiency
;;; 6. Implement improvements based on feedback
;;; 7. Convert to C or C++ library 


(library 
  (test)
  (export
    ;;;TESTING
    test-section
    reset-test-results
    test-true?
    test-equal?
    test-fail
    test-pass
    wait-dp ;wait for task queues to be empty
    print-test-report
    run-3-ut)
  (import (rnrs base))


  ;;;----------------------------------------------------------------------------
  ;;; TESTING - functions
  ;;;---------------------------------------------------------------------------- 
  ;; To wait for user input on test failure 
  ;; (define *run-3-tests-wait-before-cont* #t) 

  ;; Return #t if the given identifier is defined, else #f
  (define-syntax (defined? stx)
    (syntax-case stx ()
                 [(_ id)
                  (with-syntax ([v (identifier-binding #'id)])
                               #''v)])) 

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
        (for ([i 80])
             (printf "~a" char))
        (printf "\n"))
      (raise-inv-arg "not a string" char))) 

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
    (printf "\nNumber of test passes: ~a\n" *num-passes*)
    (printf "Number of test failures: ~a\n\n" *num-fails*)
    (printf "Failed tests:\n")
    (for ([i *failed-tests*])
         (printf "\t~a" i))
    (reset-test-results))

  ;; PUBLIC API
  ;Designate & print current test section with description
  (define (test-section name [print #t])
    (when (not (string? name))
      (raise-inv-arg "name not a string" name))
    (when (not (boolean? print))
      (raise-inv-arg "print not a boolean" print))

    (when print
      (let ()
        (set! *cur-test-section* name)
        (if (not *tests-started*)
          (let ()
            (print-test-report)
            (printf "\n\n"))
          (set! *tests-started* #t))
        (print-test-divider "#")
        (printf "### test-section ~a\n" name)
        (print-test-divider "#"))))

  ;; PUBLIC API
  ;; Return #t if the quoted form returns #t, else #f 
  (define (test-true? description form [print-result #t] [wait #f])
    (when (not (string? description))
      (raise-inv-arg "description not a string" description))
    (when (not (boolean? print-result))
      (raise-inv-arg "print-result not a boolean" print-result))
    (when (not (boolean? wait))
      (raise-inv-arg "wait not a boolean" wait))

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
              (read-line (current-input-port) 'any))))
        (let ([ret (cons #f (get-output-string o))])
          (collate-test ret)
          ret))))

  ;; PUBLIC API
  ;; Return #t if quoted forms return an equal value, else #f
  (define (test-equal? description form-a form-b [print-result #t] [wait #f])
    (when (not (string? description))
      (raise-inv-arg "description not a string" description))
    (when (not (boolean? print-result))
      (raise-inv-arg "print-result not a boolean" print-result))
    (when (not (boolean? wait))
      (raise-inv-arg "wait not a boolean" wait))

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
              (read-line (current-input-port) 'any))))
        (let ([ret (cons #f (get-output-string o))])
          (collate-test ret)
          ret))))


  ;; PUBLIC API
  ;; Custom test fail
  (define (test-fail description form [print-result #t] [wait #f])
    (when (not (string? description))
      (raise-inv-arg "description not a string" description))
    (when (not (boolean? print-result))
      (raise-inv-arg "print-result not a boolean" print-result))
    (when (not (boolean? wait))
      (raise-inv-arg "wait not a boolean" wait))

    (let ([o (open-output-string)])
      (fprintf o "FAIL TEST ~a ~a: ~a\n" (get-test-num) description form)
      (when print-result
        (printf (get-output-string o))
        (when wait
          (let ()
            (printf "<enter to continue>")
            (read-line (current-input-port) 'any))))
      (let ([ret (cons #f (get-output-string o))])
        (collate-test ret)
        ret)))


  ;; PUBLIC API
  ;; Custom test pass
  (define (test-pass description form [print-result #t])
    (when (not (string? description))
      (raise-inv-arg "description not a string" description))
    (when (not (boolean? print-result))
      (raise-inv-arg "print-result not a boolean" print-result))

    (let ([o (open-output-string)])
      (fprintf o "pass TEST ~a ~a: ~a\n" (get-test-num) description form)
      (when print-result
        (printf (get-output-string o)))
      (let ([ret (cons #t (get-output-string o))])
        (collate-test ret)
        ret)))
