(library 
  parallel
  (export task ;task record type. Pass a list of these to parallel
          parallel) ;execute all tasks in parallel before returning
  (import (chezscheme))

  (define *default-task-priority 0)
  (define *default-engine-fuel* 50)

  (define-record-type
    task
    (fields 
      (immutable priority)
      (immutable fuel)
      (immutable engine))
    (protocol 
      (lambda (new)
        (case-lambda 
          [(thunk) (new 0 *default-engine-fuel* (make-engine thunk))]
          [(thunk priority) (new priority *default-engine-fuel* (make-engine thunk))]
          [(thunk priority fuel) (new priority fuel (make-engine thunk))]
          [(engine priority fuel arbitrary) (new priority fuel engine)]))))

  (define parallel
    (lambda (tasks)
      (define priority-scheduler 
        (lambda (tasks current-priority results left)
          (if (null? tasks)
              (list tasks results)
              (let ([task (car tasks)])
                (let ([prio (task-priority task)])
                  (if (<= prio current-priority)
                      (let ([new-prio (if (< prio current-priority)
                                          prio 
                                          current-priority)]
                            [engine-ret ((task-engine task)
                                         (task-fuel task)
                                         (lambda (fuel ret-vals) '(#t ret-vals))
                                         (lambda (new-engine) '(#f new-engine)))])
                        (if (null? (car engine-ret))
                            (priority-scheduler (append (cdr tasks) (make-task new-engine prio (cadr task) #t)) new-prio results (- left 1))
                            (priority-scheduler (cdr tasks) new-prio (append res (list (cadr engine-ret))) (- left 1))))
                      (priority-scheduler (append (cdr tasks) (list task)) current-priority results (- left 1))))))))


      (define exec-tasks 
        (case-lambda 
          [(tasks) 
           (let ([priority-level (caar tasks)])
            (priority-scheduler tasks priority-level '() (length tasks)))]
          [(tasks results) 
           (let ([iter-res (priority-scheduler tasks current-priority results (length tasks))])
            (if (null? (car iter-res))
                (cadr results)
                (exec-tasks tasks results)))]))

    (exec-tasks (map make-engine tasks))))

