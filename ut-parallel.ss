#! /usr/bin/scheme --script
(let ()
  (import (chezscheme) (parallel) (test))

  (define pr #f)
  (define w #f)

    #|
    enqueue-task!
    parallel  
    go
    make-parallel-channel 
    parallel-channel-empty? 
    parallel-channel-put! 
    parallel-channel-get! 
    |# 
  (define (ut-task?)
    (test-section "make-task")

    (let ([test-task (make-task (lambda () 3))]
          [test-thunk (lambda () 3)])
      (test-true? "Make sure our task is a task record" (task? test-task) pr w)
      (test-true? "Make sure our thunk is not a task" (not (task? test-thunk)) pr w)))

  (define (ut-make-task-box)
    )

  (define (ut-task-box?)
    )
    
  (define (run-ut-parallel print-result wait)
    (set! pr print-result)
    (set! w wait)
    (ut-task?)
    (ut-make-task-box)
    (ut-task-box?))
 
  (run-ut-parallel #t #f))
