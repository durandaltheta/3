(library 
  (parallel)
  (export 
    ;  (thunk) -> task 
    ;  (thunk fuel) -> task
    ;  (engine-or-thunk fuel is-engine-flag) -> task 
    ;Make a task. Pass a list of these to make-task-box to be eventually 
    ;asynchronously executed by (parallel) or (managed-parallel)
    make-task 

    ;  (task? task) -> boolean
    task?

    ;  (make-task-box list-of-tasks) -> task-box 
    ;create a box of tasks for parallel execution from a list of task records
    make-task-box 

    ;  (enqueue-task! task-box task) -> '()
    ;enqueue a new task record to the task-box argument to be executed by 
    ;(parallel) or (managed-parallel). This works even when invoked from tasks
    ;currently being run via (parallel) or (managed-parallel).
    enqueue-task!

    ;  (parallel task-box) -> list-of-task-results 
    ;execute all tasks contained in given task-box asynchronously before 
    ;returning all task return values as a list in the order completed.
    parallel  

    ;  (make-parallel-channel) -> parallel-channel 
    ;make a channel capable of communicating between asynchronous tasks executed 
    ;by (parallel) or (managed-parallel)
    make-parallel-channel 

    ;  (parallel-channel-empty? parallel-channel) -> boolean
    parallel-channel-empty? 

    ;  (parallel-channel-put! ch any) -> '()
    parallel-channel-put! 

    ;  (parallel-channel-get! ch) -> any
    parallel-channel-get! ;blocks till data available

    ;  (make-task-manager input-source 
    ;                     get-block! 
    ;                     empty? 
    ;                     input-manager
    ;                     return-destination 
    ;                     put!) -> task-manager 
    ;
    ;  breakdown of arguments, where (-> *) means a function that takes * as an 
    ;  argument:
    ;     input-source : arbitrary-communication-object 
    ;     get-block! : (-> input-source) -> any 
    ;     empty? : (-> input-source) -> boolean 
    ;     input-manager : (-> any) -> task-or-null 
    ;     return-destination : arbitrary-communication-object
    ;     put! : (-> any) -> null
    ;
    ;a record of arbitrary input/output communication objects (can be queue, 
    ;channel, etc.) and manipulation functions for use in (managed-parallel). 
    ;
    ;Allows for continual accrual of input into running tasks in 
    ;(managed-parallel) without blocking execution or busy waiting. 
    ;
    ;Allows input to be processed before being executed via argument function 
    ;input-manager. Input-manager takes input from (get-block! input-source) 
    ;and outputs a task or '()
    make-task-manager 

    ;  (task-manager? task-manager) -> boolean 
    task-manager? 

    ;  (managed-parallel task-box task-manager) -> list-of-task-results 
    ;same as parallel but uses a task-manager to continuously gather and 
    ;enqueue new tasks using input from an arbitrary communication channel. 
    ;Does not return until #f is received from input-source. Useful when 
    ;getting tasks/input from another thread or input from another process
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
      (if (null? task-list)
          (list #t '())
          (if (task? (car task-list))
              (validate-task-list (cdr task-list))
              (list #f (car task-list)))))

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


  (define (dequeue-task! task-box) ;null? checking happens in (parallel)
    (let ([tasks (unbox task-box)])
      (let ([task (car tasks)])
        (set-box! task-box (cdr tasks))
        task)))


  ;global placeholders
  (define *CHANNEL-BLOCK-FLAG* #f)
  (define *CHANNEL-BLOCK-ID* 0)
  (define *CHANNEL-NON-EMPTY-FLAG* #f)
  (define *CHANNEL-NON-EMPTY-IDS* '())

  (define (set-block! val bool)
    (set! *CHANNEL-BLOCK-ID* val)
    (set! *CHANNEL-BLOCK-FLAG* bool))

  (define (append-non-empty! val bool)
    (set! *CHANNEL-NON-EMPTY-IDS* (append *CHANNEL-NON-EMPTY-IDS* (list val)))
    (set! *CHANNEL-NON-EMPTY-FLAG* bool))

  ;function that always emits a unique numerical value per invocation
  (define incrementor (let ([id 0]) (lambda () (let ([ret id]) (set! id (+ id 1)) ret))))

  (define-record-type
    parallel-channel
    (fields 
      (mutable data)
      (immutable id)
      (immutable put!)
      (immutable get!))
    (protocol
      (lambda (new)
        (lambda () 
          (let* ([data '()]
                 [id (incrementor)])
            (new 
              data
              id
              (lambda (val) 
                (append-non-empty! id #t)
                (set! data (append data (list val))))
              (lambda ()
                (define (loop)
                  (if (null? data)
                      (let ()
                        (set-block! id #t)
                        (engine-block)
                        (loop))
                      (let ([ret (car data)])
                        (set! data (cdr data)))))
                (loop))))))))

  (define (parallel-channel-empty? pch)
    (null? (parallel-channel-data pch)))


  ;execute tasks in task-box in parallel
  (define (parallel task-box)

    ;temporarily assign new values for global variables
    (fluid-let 
      ([*CHANNEL-BLOCK-FLAG* #f]
       [*CHANNEL-BLOCK-ID* 0]
       [*CHANNEL-NON-EMPTY-FLAG* #f]
       [*CHANNEL-NON-EMPTY-IDS* 0])

      ;list of waiting tasks
      (define waiting-tasks '())

      (define (enqueue-waiting-task! task)
        (set! waiting-tasks (append waiting-tasks (list *CHANNEL-BLOCK-ID* task)))
        (set-block! 0 #f))

      ;enqueue any tasks waiting for now non-empty channel id
      (define (enqueue-tasks-waiting-for-id! id)
        (define (loop waiting)
          (if (null? waiting)
              '()
              (let ([cur (car waiting)])
                (when (eqv? id (car cur))
                  (enqueue-task! task-box (cadr cur))
                  (set! waiting-tasks (remv cur waiting-tasks)))
                (loop (cdr waiting)))))
        (loop waiting-tasks))

      ;handle all now non-empty channel ids
      (define (enqueue-waiting-tasks!)
        (map enqueue-waiting-tasks! *CHANNEL-NON-EMPTY-IDS*)
        (set! *CHANNEL-NON-EMPTY-FLAG* #f)
        (set! *CHANNEL-NON-EMPTY-IDS* '()))

      ;execute tasks in parallel
      (define (exec-tasks task-box results)
        (if (null? (unbox task-box))
            results
            (let ([task (dequeue-task! task-box)])
              (let ([fuel (task-fuel task)])
                (let ([engine-ret ((task-engine task)
                                   fuel
                                   (lambda (fuel ret-vals) '(#t ret-vals))
                                   (lambda (new-engine) '(#f new-engine)))])
                  (when *CHANNEL-NON-EMPTY-FLAG* (enqueue-waiting-tasks!))
                  (if (car engine-ret)
                      (exec-tasks task-box (append results (list (cadr engine-ret))))
                      (let ()
                        (if *CHANNEL-BLOCK-FLAG* 
                            (enqueue-waiting-task! (make-task (cadr engine-ret) fuel))
                            (enqueue-task! task-box (make-task (cadr engine-ret) fuel #t)))
                        (exec-tasks task-box results))))))))
      (exec-tasks task-box '())))


  (define-record-type 
    task-manager 
    (fields
      ;some kind of communication object of arbitrary implementation: queue, 
      ;channel, etc. Further tasks/thunks to evaluate are gathered here. 
      ;managed-parallel ends collecting tasks when a #f is received from this 
      ;object
      (mutable input-source) 

      ;function that takes one argument (input-source) and blocks 
      ;if nothing available to return, otherwise returns obtained value
      (immutable get-block!) 

      ;returns #t if at least one value ready to be gathered from get-block!,
      ;otherwise returns #f
      (immutable empty?)

      ;DISABLE FUNCTIONALITY BY PROVIDING #f AS INPUT
      ;A function that takes one argument. Output from this function 
      ;is placed in the task-box with enqueue-task! Pass #f to this field if 
      ;this functionality is not desired 
      ;
      ;This is useful when input from input-source does not come in the form 
      ;of an executable task, and must be interpreted (for instance, when 
      ;input-source is a cross-process communication queue).
      (immutable input-manager)

      ;DISABLE FUNCTIONALITY BY PROVIDING #f AS INPUT
      ;The following fields are not required to be provided as function 
      ;arguments. However, both must be absent or provided together, record 
      ;creation will fail if only one field is given

      ;DISABLE FUNCTIONALITY BY PROVIDING #f AS INPUT
      ;Some kind of communication object of arbitrary implementation: 
      ;queue, channel, etc. Results from thunk execution are placed here 
      ;when parallel internally returns a batch of results. 
      ;
      ;If a more asynchronous solution is required, simply have your tasks 
      ;return their results internally to your communication object. This is 
      ;threadsafe because parallel doesn't use threads!
      (mutable result-destination) 

      ;function that puts a value into a communication object. If 
      ;this operation does not block, may lose computation results, but is up 
      ;to implementation 
      (immutable put!)) 
    (protocol 
      (lambda (new)
        (lambda (input-source get-block! empty? input-manager result-destination put!)
          (new input-source get-block! empty? input-manager result-destination put!)))))


  ;Same as (parallel) except it can continuously take new input from outside 
  ;the current thread or process using a task-manager record. Gathering input 
  ;will only block if no tasks are executing, otherwise it gathers input 
  ;periodically without blocking.
  (define (managed-parallel task-box task-manager)
    (define (run)
      (let ([collecting #t]
            [input-source (task-manager-input-source task-manager)]
            [input-manager (task-manager-input-manager task-manager)]
            [result-destination (task-manager-result-destination task-manager)]
            [get-block! (task-manager-get-block! task-manager)]
            [empty? (task-manager-empty? task-manager)]
            [put! (task-manager-put! task-manager)])

        ;;---------------------------------------------------------------------
        ;;DEFINITIONS
        ;;---------------------------------------------------------------------
        ;If input == #f, stop collecting input this invocation of 
        ;managed-parallel
        (define (handle-input! input)
          (if input 
              (if input-manager 
                  (let ([processed-input (input-manager input)])
                    (when processed-input
                      (enqueue-task! task-box (input-manager input))))
                  (enqueue-task! task-box input))
              (set! collecting #f)))

        ;attempt to get input and put input into task-box. If no input found 
        ;and task-box empty, block. 
        (define (get-input block)
          (if collecting
              (let ([input (if (empty? input-source)
                               '()
                               (get-block! input-source))])
                (if (null? input)
                    (if (and (null? (unbox task-box)) block)
                        (let ()
                          (handle-input! (get-block! input-source))
                          (get-input block))
                        '())
                    (let ()
                      (handle-input! input)
                      (get-input block))))
              '()))


        ;Re-enqueueing task that attempts to get more input unless no 
        ;longer collecting input or it is the only task 
        (define (parallel-get-input)
          (get-input #f)
          (when (and collecting (> (length (unbox task-box)) 0))
            (enqueue-task! task-box (make-task parallel-get-input))))

        ;function to handle results of parallel
        (define (put-results results)
          (if (and result-destination put!)
              (let ([res (car results)])
                (when (not (null? res))
                  (put! result-destination (car results)))
                (if (null? (cdr results))
                    '()
                    (put-results (cdr results))))
              '()))

        ;;---------------------------------------------------------------------
        ;;EXECUTION
        ;;---------------------------------------------------------------------
        ;If we're still collecting input, add and input collection task to the 
        ;task-box 
        (when collecting (enqueue-task! task-box (make-task parallel-get-input)))

        ;Execute all tasks and handle results, temporarily setting 'executing'
        ;flag to #t
        (put-results (parallel task-box))

        ;If still collecting, recurse this function
        (if collecting
            (let ()
              ;Attempt to get more input (blocking if no input and no tasks)
              (get-input #t)
              (run))
            #t)))
    (run)))
