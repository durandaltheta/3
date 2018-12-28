(library
  (ut-test)
  (export run-ut-test)
  (import (chezscheme)
          (test))

  (define (ut-test-section)
    (test-section "ut-test-section-1")
    (test-section "ut-test-section-2"))

  (define (ut-test-true?)
    (test-section "ut-test-true?")
    (test-true? "boolean value case 1" #t)
    (test-true? "boolean value case 1-2" #f)
    (test-true? "boolean value case 2" #t #t)
    (test-true? "boolean value case 2-2" #t #f)
    (test-true? "boolean value case 3" #t #t #t)
    (test-true? "boolean value case 3-2" #t #t #f)
    (test-true? "boolean value case 3-3" #f #t #t)
    (test-true? "boolean value case 3-4" #f #t #f))

  (define (ut-test-equal?)
    (test-section "ut-test-equal?")

    )


  #|
  (define (ut-print-test-report)
    )

  (define (ut-reset-test-results)
    )
  |#

  (define (run-ut-test)
    (ut-test-section)
    (ut-test-true?)))


