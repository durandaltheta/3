(library 
  (ut-parallel)
  (export run-ut-parallel)
  (import (chezscheme) (parallel) (test))

  (define pr #f)
  (define w #f)


    #|
    ;tests:
    make-task 
    task?
    make-task-box 
    enqueue-task!
    parallel  
    make-parallel-channel 
    parallel-channel-empty? 
    parallel-channel-put! 
    parallel-channel-get! 
    make-task-manager 
    task-manager? 
    managed-parallel
    |# 
  (define (ut-task?)
    (test-section)

    (let ([test-task (make-task (lambda () 3))]
          [test-thunk (lambda () 3)])
      (test-true? "Make sure our task is a task record" (task? test-task) pr w)
      (test-true? "Make sure our thunk is not a task" (not (task? test-thunk)) pr w)))
    
  (define (run-ut-parallel print-result wait)
    (set! pr print-result)
    (set! w wait)
    (ut-task?)
    (print-test-report)))
