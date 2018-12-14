(library 
  parallel
  (export 
    ;task record type. Pass a list of these to parallel
    task  

    ;create a box of tasks for parallel execution
    make-task-box 

    ;enqueue a new task record to the current task box. This works even from 
    ;within running tasks
    enqueue-task!

    ;execute all tasks contained in given task-box in parallel before returning
    ;all task return values as a list in the order completed.
    parallel  

    ;a record of input/output communication objects and manipulation functions
    task-manager 

    ;same as parallel but uses a task-manager to continuously gather and 
    ;enqueue new tasks. Does not return until #f is received from input-source. 
    ;Only useful when getting tasks from another thread or from another process
    managed-parallel) 
  (import (chezscheme))

  (define *default-task-priority 0)
  (define *default-engine-fuel* 50)

  (define-record-type
    task
    (fields 
      (immutable fuel)
      (immutable engine))
    (protocol 
      (lambda (new)
        (case-lambda 
          [(thunk) (new *default-engine-fuel* (make-engine thunk))]
          [(thunk fuel) (new fuel (make-engine thunk))]
          [(engine-or-thunk fuel is-engine) 
           (if is-engine (new fuel engine-or-thunk)
               (new fuel (make-engine engine-or-thunk)))]))))


  (define (make-task-box task-list)
    (define (validate-task-list task-list)
      (if (null? lst)
          (list #t '())
          (if (task? (car lst))
              (validate-task-list (cdr lst))
              (list #f (car lst)))))

    (let ([res (validate-task-list task-list)])
      (if (car res)
          (box task-list)
          (let ()
            (fprintf (current-error-port) 
                     "[make-task-box] provided task ~a is not a task record, in task list: ~a; task thrown out\n" 
                     (cadr res) 
                     task-list)
            (box '())))))


  (define (enqueue-task! task-box task)
    (if (task? task)
        (set-box! task-box (append (unbox task-box) (list task)))
        (fprintf (current-error-port) 
                 "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)))


  (define (dequeue-task! task-box)
    (let ([tasks (unbox task-box)])
      (let ([task (car tasks)])
        (set-box! task-box (cdr tasks))
        task)))


  (define-record-type 
    task-manager 
    (field 
      ; some kind of communication object of arbitrary implementation: queue, 
      ; channel, etc. Further tasks/thunks to evaluate are gathered here. 
      ; managed-parallel ends collecting tasks when a #f is received from this 
      ; object
      (mutable input-source) 

      ; A function that takes one argument. Output from this function 
      ; is placed in the task-box with enqueue-task! Pass #f to this field if 
      ; this functionality is not desired
      (immutable input-manager)

      ; function that takes one argument (input-source) and blocks 
      ; if nothing available to return, otherwise returns obtained value
      (immutable get-block!)

      ; function that takes one argument (input-source) and returns 
      ; 'empty if nothing available to return, otherwise returns obtained value
      (immutable get-try!) 

      ;;OPTIONAL FIELDS:
      ;; The following fields are not required. However, both must be absent or 
      ;; provided together, record creation will fail if only one field is 
      ;; given

      ; Some kind of communication object of arbitrary implementation: 
      ; queue, channel, etc. Results from thunk execution are placed here 
      ; individually when parallel returns a batch of results. 
      ;
      ; If a more asynchronous solution is required, simply have your tasks 
      ; return their results internally to your communication object. This is 
      ; threadsafe because parallel doesn't use threads!
      (mutable result-destination) 

      ;function that puts a value into a communication object. If 
      ;this operation does not block, may lose computation results, but is up 
      ;to implementation 
      (immutable put!)) 
    (protocol 
      (lambda (new)
        (case-lambda 
          [(input-source input-manager get-block! get-try! result-destination put!)
           (new input-source get-block! get-try! result-destination put!)]
          [(input-source input-manager get-block! get-try!)
           (new input-source input-manager get-block! get-try! #f #f)]))))


  ;block if only task 
  ;do not block if more than one task 
  (define (parallel task-box)
    (define (exec-tasks task-box results)
      (if (null? (unbox task-box))
          results
          (let ([task (dequeue-task! task-box)])
            (let ([fuel (task-fuel task)])
              (let ([engine-ret ((task-engine task)
                                 fuel
                                 (lambda (fuel ret-vals) '(#t ret-vals))
                                 (lambda (new-engine) '(#f new-engine)))])
                (if (car engine-ret)
                    (exec-tasks task-box
                                (append results (list (cadr engine-ret))))
                    (let ()
                      (enqueue-task! task-box (make-task (cadr engine-ret) fuel #t))
                      (exec-tasks task-box results ))))))))
    (exec-tasks task-box '()))


  (define (managed-parallel task-box task-manager)
    (define (run)
      (let ([collecting #t]
            [input-source (task-manager-input-source task-manager)]
            [input-manager (task-manager-input-manager task-manager]
            [result-destination (task-manager-result-destination task-manager)]
            [get-block! (task-manager-get-block! task-manager)]
            [get-try! (task-manager-get-try! task-manager)]
            [put! (task-manager-put! task-manager)])

        ;If input == #f, stop collecting input this invocation of 
        ;managed-parallel
        (define (handle-input! input)
          (if input 
              (if input-manager
                (enqueue-task! task-box (input-manager input))
                (enqueue-task! task-box input))
              (set! collecting #f)))

        ;attempt to get input and put input into task-box. If no input found 
        ;and task-box empty, block. 
        (define (get-input block)
          (if collecting
              (let ([input (get-try! input-source)])
                (if (eqv? input 'empty)
                    (if (and (null? (unbox task-box)) block)
                        (let ()
                          (handle-input! (get-block! input-source))
                          (get-input block))
                        '())
                    (let ()
                      (handle-input! input)
                      (get-input block))))
              '()))

        ;If we're still collecting input, add and input collection task to the 
        ;task-box
        (when collecting (enqueue-thunk! task-box (lambda () (get-input #f))))

        ;Handle results of parallel
        (define (put-results results)
          (if (and return-destination put!)
              (let ([res (car results)])
                (when (not (null? res))
                  (put! result-destination (car results)))
                (if (null? (cdr results))
                    '()
                    (put-results (cdr results))))
              '()))

        ;Execute all tasks and handle results, temporarily setting 'executing'
        ;flag to #t
        (fluid-let ([executing #t]) (put-results (parallel task-box)))

        ;If still collecting, recurse this function
        (if collecting
            (let ()
              ;Attempt to get more input (blocking if no input and no tasks)
              (get-input #t)
              (run))
            #t)))
    (run)))
