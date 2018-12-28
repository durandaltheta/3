#! /usr/bin/scheme --script
(let ()
  (import (chezscheme) (test))

  (define (ut-test-section)
    (test-section "ut-test-section-1")
    (print-test-report)
    (test-section "ut-test-section-2")
    (print-test-report))

  (define (ut-test-true?)
    (test-section "ut-test-true?")
    (test-true? "boolean case 1, true" #t)
    (test-true? "boolean case 1-2, false" #f)
    (test-true? "boolean case 1-3" (not #f))
    (test-true? "boolean case 2" #t #t)
    (test-true? "boolean case 2-2" #t #f)
    (test-true? "boolean case 3" #t #t #t)
    (test-true? "boolean case 3-2" #t #t #f)
    (test-true? "boolean case 3-3, wait on fail" #f #t #t)
    (test-true? "boolean case 3-4" #f #t #f)
    (print-test-report))

  (define (ut-test-equal?)
    (test-section "ut-test-equal?")
    (test-equal? "string case 1, string equal" "foo" "foo")
    (test-equal? "string case 1-2, string not equal" "foo" "faa")
    (test-equal? "string case 2, string equal" "foo" "foo" #t)
    (test-equal? "string case 2-2, string equal" "foo" "foo" #f)
    (test-equal? "string case 3, string not equal" "foo" "faa" #t #t)
    (test-equal? "string case 3-2, string not equal" "foo" "faa" #t #f)

    (let ([test-list (list 1 2 5)]
          [test-list2 (list 2 5 6)])
      (test-equal? "lists equal" test-list test-list)
      (test-equal? "lists not equal" test-list test-list2))

    (test-equal? "numbers equal" 3 3)
    (test-equal? "numbers not equal" 3 21)

    (print-test-report))


  (define (ut-print-test-report)
    (test-section "ut-print-test-report 1")
    (test-equal? "num equal" 3 4)
    (test-equal? "num equal" 3 5)
    (test-section "ut-print-test-report 2")
    (test-equal? "num equal" 3 6)
    (print-test-report))

  (define (ut-reset-test-results)
    (test-section "ut-reset-test-results 1")
    (test-equal? "num equal" 3 4)
    (test-equal? "num equal" 3 5)
    (reset-test-results)
    (test-section "ut-reset-test-results 2")
    (test-equal? "num equal" 3 6)
    (print-test-report))

  (define (run-ut-test)
    (ut-test-section)
    (ut-test-true?)
    (ut-test-equal?)
    (ut-print-test-report)
    (ut-reset-test-results))
  (run-ut-test))