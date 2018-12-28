(library 
  (parallel)
  (export 
    ;  (make-task thunk) -> task 
    ;  (make-task thunk fuel) -> task
    ;  (make-task engine-or-thunk fuel is-engine-flag) -> task 
    ;Make a task. Pass a list of these to make-task-box to be eventually 
    ;asynchronously executed by (parallel) or (managed-parallel)
    make-task 

    ;  (task? a) -> boolean
    ;returns #t if argument a is a task, else returns #f
    task?

    ;  (make-task-box list-of-tasks) -> task-box 
    ;create a box of tasks for parallel execution from a list of task records
    make-task-box 

    ;  (task-box? a) -> boolean
    ;returns #t if argument a is a task-box, else returns #f
    task-box?

    ;  (enqueue-task! task-box task) -> (values)
    ;enqueue a new task record to the task-box argument to be executed by 
    ;(parallel) or (managed-parallel). This works even when invoked internally 
    ;from tasks currently being run via (parallel) or (managed-parallel).
    enqueue-task!

    ;  (parallel task-box) -> list-of-task-results 
    ;execute all tasks contained in given task-box asynchronously before 
    ;returning all task return values as a list in the order completed.
    parallel  

    ;  (make-parallel-channel) -> parallel-channel  
    ;make a channel capable of communicating between asynchronous tasks 
    ;executed by (parallel) or (managed-parallel).
    ;
    ;These channels are *ONLY* safe to use within tasks running in (parallel) 
    ;or (managed-parallel).
    ;
    ;
    ;  (make-parrallel-channel communication-object blocking-get! put! empty?)
    ;It is possible to create a parallel channel by passing this function an 
    ;arbitrary communication object (a queue, channel, pipe, etc..) and the 
    ;following object manipulation functions:
    ;  blocking-get! : returns a value from the object. Blocks if nothing in 
    ;                  the object.
    ;  put! : puts a value into the object. If this blocks, it will block
    ;         the entire thread (including parallel/managed-parallel)
    ;  empty? : returns #t if object is empty, else #f
    make-parallel-channel 

    ;  (parallel-channel-empty? parallel-channel) -> (->) -> boolean 
    ;  Returns a function that returns #t if values are waiting in the channel 
    ;  otherwise returns false.
    parallel-channel-empty? 

    ;  (parallel-channel-put! ch any) -> (-> any) -> (values)
    ;  Returns a function that will accept a value and place it into the 
    ;  parallel-channel.
    parallel-channel-put! 

    ;  (parallel-channel-get! ch) -> (->) -> any
    ;  Returns a function that will return a value from the parallel-channel.
    parallel-channel-get! ;blocks till data available

    ;  (make-task-manager input-parallel-channel
    ;                     output-parallel-channel
    ;                     input-manager) -> task-manager 
    ;
    ;  breakdown of arguments, where (-> *) means a function that takes * as an 
    ;  argument:
    ;     input-parallel-channel : parallel-channel where data is expected to 
    ;                              come from outside the thread or process
    ;     output-parallel-channel : parallel-channel where data task output 
    ;                               is sent
    ;     input-manager : (-> any) -> task-or-null 
    ;
    ;Allows for continual accrual of input into running tasks in 
    ;(managed-parallel) without blocking execution or busy waiting. 
    ;
    ;Allows input to be processed before being executed via argument function 
    ;input-manager. input-manager takes input from input-parallel-channel
    ;and outputs a task (to be executed in parallel). All output values that 
    ;are not tasks are ignored.
    ;
    ;output-parallel-channel and input-manager are optional functionality. If 
    ;said functionality is not desired, pass #f to any/all of these fields
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


  (define-record-type
    task-box 
    (fields (mutable contents))
    (protocol 
      (lambda (new) 
        (lambda (task-list)
          (define (validate-task-list task-list)
            (if (null? task-list)
              (list #t '())
              (if (task? (car task-list))
                (validate-task-list (cdr task-list))
                (list #f (car task-list)))))

          (let ([res (validate-task-list task-list)])
            (if (car res)
              (new (box task-list))
              (let ()
                (fprintf (current-error-port) 
                         "[error make-task-box] provided task ~a is not a task record, in task list: ~a\n" 
                         (cadr res) 
                         task-list)
                (new (box '())))))))))


  (define (enqueue-task! task-box task)
    (if (task? task)
      (set-box! task-box (append (task-box-contents (unbox task-box)) (list task)))
      (fprintf (current-error-port) 
               "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)))


  (define (dequeue-task! task-box) ;null? checking happens in (parallel)
    (let ([tasks (task-box-contents (unbox task-box))])
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

  (define (append-non-empty! val)
    (set! *CHANNEL-NON-EMPTY-IDS* (append *CHANNEL-NON-EMPTY-IDS* (list val)))
    (set! *CHANNEL-NON-EMPTY-FLAG* #t))

  ;function that always emits a unique numerical value per invocation
  (define incrementor (let ([id 0]) 
                        (lambda () (let ([ret id]) 
                                     (set! id (+ id 1)) 
                                     ret))))

  (define-record-type
    parallel-channel
    (fields 
      (mutable data)
      (immutable id)
      (immutable get!)
      (immutable put!) ;if this blocks the whole thread will block
      (immutable empty?))
    (protocol
      (lambda (new)
        [id (incrementor)])
      (case-lambda 
        [(data get! put! empty!)
         (new data id get! put! empty?)]
        [() 
         (let ([data '()]
                 (new 
                   data
                   id
                   (lambda () ;default get!
                     (define (loop)
                       (if (null? data)
                         (let ()
                           (set-block! id #t)
                           (engine-block)
                           (loop))
                         (let ([ret (car data)])
                           (set! data (cdr data)))))
                     (loop))
                   (lambda (val) ;default put!
                     (set! data (append data (list val)))
                     (append-non-empty! id))
                   (lambda () (null? data)))))]))) ;default empty?


  ;execute tasks in task-box in parallel
  (define (parallel tb)
    (if (task-box? tb)
      ;temporarily assign new values for global variables
      (fluid-let 
        ([*CHANNEL-BLOCK-FLAG* #f]
         [*CHANNEL-BLOCK-ID* 0]
         [*CHANNEL-NON-EMPTY-FLAG* #f]
         [*CHANNEL-NON-EMPTY-IDS* 0])

        ;list of waiting tasks
        (define waiting-tasks '())

        (define (enqueue-waiting-task! task)
          (set! waiting-tasks (append waiting-tasks 
                                      (list *CHANNEL-BLOCK-ID* task)))
          (set-block! 0 #f))

        ;enqueue any tasks waiting for now non-empty channel id
        (define (enqueue-tasks-waiting-for-id! id)
          (define (loop waiting)
            (if (null? waiting)
              '()
              (let ([cur (car waiting)])
                (when (eqv? id (car cur))
                  (enqueue-task! tb (cadr cur))
                  (set! waiting-tasks (remv cur waiting-tasks)))
                (loop (cdr waiting)))))
          (loop waiting-tasks))

        ;handle all now non-empty channel ids
        (define (enqueue-waiting-tasks!)
          (map enqueue-waiting-tasks! *CHANNEL-NON-EMPTY-IDS*)
          (set! *CHANNEL-NON-EMPTY-FLAG* #f)
          (set! *CHANNEL-NON-EMPTY-IDS* '()))

        ;execute tasks in parallel
        (define (exec-tasks tb results)
          (if (null? (tax-box-contents (unbox tb)))
            results
            (let ([task (dequeue-task! tb)])
              (let ([fuel (task-fuel task)])
                (let ([engine-ret ((task-engine task)
                                   fuel
                                   (lambda (fuel ret-vals) '(#t ret-vals))
                                   (lambda (new-engine) '(#f new-engine)))])
                  (when *CHANNEL-NON-EMPTY-FLAG* (enqueue-waiting-tasks!))
                  (if (car engine-ret)
                    (exec-tasks tb (append results (list (cadr engine-ret))))
                    (let ()
                      (if *CHANNEL-BLOCK-FLAG* 
                        (enqueue-waiting-task! (make-task (cadr engine-ret) fuel))
                        (enqueue-task! tb (make-task (cadr engine-ret) fuel #t)))
                      (exec-tasks tb results))))))))
        (exec-tasks tb '()))
      (let ()
        (fprintf 
          (current-error-port) 
          "[error: parallel] provided task-box argument ~a is not a task-box record\n" 
          tb))))


  (define-record-type 
    task-manager 
    (fields
      (mutable input-parallel-channel) 

      ;DISABLE FUNCTIONALITY BY PROVIDING #f AS INPUT
      (mutable output-parallel-channel)

      ;DISABLE FUNCTIONALITY BY PROVIDING #f AS INPUT
      (immutable input-manager))
    (protocol 
      (lambda (new)
        (lambda (input-parallel-channel output-parallel-channel input-manager)
          (new input-parallel-channel output-parallel-channel input-manager)))))


  ;Same as (parallel) except it can continuously take new input from outside 
  ;the current thread or process using a task-manager record. Gathering input 
  ;will only block if no tasks are executing, otherwise it gathers input 
  ;periodically without blocking.
  (define (managed-parallel task-box task-manager)
    (define (run)
      (let ([collecting #t]
            [input-source (task-manager-input-source task-manager)]
            [input-manager (task-manager-input-manager task-manager)]
            [result-destination (parallel-channel-data (task-manager-output-parallel-channel task-manager))]
            [get-block! (parallel-channel-get-block! (task-manager-input-parallel-channel task-manager))]
            [empty? (parallel-channel-get-empty? (task-manager-input-parallel-channel task-manager))]
            [put! (parallel-channel-put! (task-manager-output-parallel-channel task-manager))])

        ;;---------------------------------------------------------------------
        ;;DEFINITIONS
        ;;---------------------------------------------------------------------
        ;If input == #f, stop collecting input this invocation of 
        ;managed-parallel
        (define (handle-input! input)
          (if input 
            (if input-manager 
              (let ([processed-input (input-manager input)])
                (when (task? processed-input)
                  (enqueue-task! task-box processed-input)))
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
            (put! result-destination results)
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
    (if (task-box? tb)
      (run)
      (let ()
        (fprintf 
          (current-error-port) 
          "[error: managed-parallel] provided task-box argument ~a is not a task-box record\n" 
          tb)))))
