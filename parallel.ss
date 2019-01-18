(library 
  (parallel)
  (export 
    ;  (make-task thunk) -> task 
    ;  (make-task thunk fuel) -> task
    ;  (make-task engine-or-thunk fuel is-engine-flag) -> task 
    ;Make a task. Pass a list of these to make-task-box to be eventually 
    ;asynchronously executed by (parallel)
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

    ;  (enqueue-task! task-box task) -> void
    ;enqueue a new task record to the task-box argument to be executed by 
    ;(parallel). This works even when invoked internally 
    ;from tasks currently being run via (parallel) or.
    enqueue-task!

    ;  (set-unsafe!) -> void
    ;When this mode is active it forces (parallel) to execute linearly 
    ;(disables asynchronous execution). This makes modification to 
    ;shared/global values safe. parallel-channels use these internally.
    set-unsafe!

    ;  (set-safe!) -> void
    ;Enable parallel execution in (parallel). This is #t by default and only 
    ;set #f when (set-unsafe!) is called.
    set-safe!

    ;Returns #t if in unsafe mode, otherwise #f
    unsafe?

    ;  (make-parallel-channel) -> parallel-channel  
    ;make a channel capable of communicating between asynchronous tasks 
    ;executed by (parallel) or (managed-parallel).
    ;
    ;These channels are *ONLY* safe to use within tasks running in (parallel).
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

    ;  (parallel-channel? a) -> boolean
    ;returns #t if argument a is a parallel-channel, else returns #f
    parallel-channel?

    ;  (ch-get parallel-channel) -> any
    ;Get value from parallel channel
    ch-get!

    ;  (ch-put parallel-channel any) -> void
    ;Put a value into a parallel-channel
    ch-put!

    ;  (ch-empty? parallel-channel) -> boolean
    ;Returns #t if channel is empty, else returns #f
    ch-empty?

    ;  (parallel task-box) -> list-of-task-results 
    ;execute all tasks contained in given task-box asynchronously before 
    ;returning all task return values as a list in the order completed. 
    ;
    ;WARNINGS:
    ;- Will not return if any tasks are blocking via parallel-channel-get!
    ;- Do *not* use (set-timer) inside a task, will break implementation
    ;- To make modification of shared memory safe, invoke (set-unsafe!). This 
    ;  forces parallel to execute linearly. Use (set-safe!) to restore normal
    ;  asynchronous execution.
    parallel  

    ;  (go thunk) -> void
    ;  (go thunk fueld) -> void
    ;Enqueues a thunk to be executed. (go) is special because it uses a global 
    ;task box managed in the background. This is the easiest way to run tasks 
    ;asynchronously. As a note these tasks will not be executed unless 
    ;(start) is called.
    go

    ;  (start) -> list-of-task-results
    ;starts parallel if  it is not already running. As a note,
    ;if parallel is *not* running, this function will block until parallel
    ;returns. However, any (go) function executed by a running task will *NOT*
    ;block, therefore this is safe to use within any task started by (go).
    ;
    ;Normal usage is to queue up some tasks with (go), then execute (start). 
    ;Any (go) calls used within the queued tasks will also execute.
    start)
  (import (chezscheme)) 

  (define (raise-error msg)
    (raise
      (condition
        (make-error)
        (make-message-condition msg))))

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
    (fields (mutable contents)
            (mutable waiting)
            (mutable running))
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
                (new (box task-list) '() #f)
                (let ([o (open-output-string)])
                  (fprintf o
                           "[make-task-box] provided task ~a is not a task record, in task list: ~a\n" 
                           (cadr res) 
                           task-list)
                  (raise-error (get-output-string o))
                  (new (box '()) '() #f))))))))


  (define (enqueue-task! tb task)
    (if (task? task)
        (set-box! (task-box-contents tb) (append (unbox (task-box-contents tb)) (list task)))
        (let ([o (open-output-string)])
          (fprintf o "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)
          (raise-error (get-output-string o)))))


  (define (dequeue-task! tb) ;null? checking happens in (parallel)
    (let ([tasks (unbox (task-box-contents tb))])
      (let ([task (car tasks)])
        (set-box! (task-box-contents tb) (cdr tasks))
        task)))


  (define (enqueue-task-as-head! tb task)
    (if (task? task)
        (set-box! (task-box-contents tb) (append (list task) (unbox (task-box-contents tb))))
        (let ([o (open-output-string)])
          (fprintf o "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)
          (raise-error (get-output-string o)))))


  ;global task timeout 'mutex'
  (define *UNSAFE-TIMEOUT* #f)
  (define (set-unsafe!) (set! *UNSAFE-TIMEOUT* #t))
  (define (set-safe!) (set! *UNSAFE-TIMEOUT* #f))
  (define (unsafe?) *UNSAFE-TIMEOUT*)


  ;global channel flags and lists
  (define *CHANNEL-BLOCK-FLAG* #f)
  (define *CHANNEL-BLOCK-ID* 0)
  (define *CHANNEL-NON-EMPTY-FLAG* #f)
  (define *CHANNEL-NON-EMPTY-IDS* (list))


  (define (set-block! val bool)
    (set-unsafe!)
    (set! *CHANNEL-BLOCK-ID* val)
    (set! *CHANNEL-BLOCK-FLAG* bool)
    (set-safe!))

  ;forward declaration
  (define append-non-empty-channel-id! 
    (lambda (val)
      (set-unsafe!)
      (printf "*CHANNEL-NON-EMPTY-IDS* 1: ~a\n" *CHANNEL-NON-EMPTY-IDS*)
      (flush-output-port)
      (set! *CHANNEL-NON-EMPTY-IDS* (append *CHANNEL-NON-EMPTY-IDS* (list val)))
      (printf "*CHANNEL-NON-EMPTY-IDS* 2: ~a\n" *CHANNEL-NON-EMPTY-IDS*)
      (flush-output-port)
      (set! *CHANNEL-NON-EMPTY-FLAG* #t)
      (set-safe!)))

  ;function that always emits a unique numerical value per invocation
  (define incrementor (let ([id 0]) 
                        (lambda () (let ([ret id]) 
                                     (set! id (+ id 1)) 
                                     ret))))

  ;record that contains threadsafe data passing functions. Can be created 
  ;without arguments to create a default record OR with custom communication 
  ;object and blocking-get!, put!, and empty? functions that operate on said 
  ;communication object.
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
        (case-lambda 
          [(data get! put! empty?) ;custom parallel-channel case 
           (let ([id (incrementor)])
             (printf "!!!!!!!!!!!!id: ~a\n" id)
             (flush-output-port)
             (new 
               data 
               id 
               (lambda () ;custom get!
                 (define (loop)
                   (if (empty? data)
                       (let ()
                         (set-block! id #t)
                         (engine-block)
                         (loop))
                       (get! data)))
                 (loop))
               (lambda (val) ;custom put!
                 (put! data val)
                 (append-non-empty-channel-id! id))
               empty?))] ;custom empty?
          [() ;default parallel-channel case
           (let ([data (list)]
                 [id (incrementor)])
             (printf "!!!!!!!!!!!!id: ~a\n" id)
             (flush-output-port)
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
                         (set! data (cdr data))
                         ret)))
                 (loop))
               (lambda (val) ;default put!
                 (set! data (append data (list val)))
                 (append-non-empty-channel-id! id))
               (lambda () (null? data))))])))) ;default empty?

  ;PUBLIC API
  ;  (ch-get! ch) -> any
  (define (ch-get! ch)
    (if (parallel-channel? ch)
        ((parallel-channel-get! ch))
        (let ([o (open-output-string)])
          (fprintf o "[ch-get!] provided ch ~a is not a parallel-channel record\n" ch)
          (raise-error (get-output-string o))
          (values))))

  ;PUBLIC API
  ;  (ch-put! ch val) -> void
  (define (ch-put! ch val)
    (if (parallel-channel? ch)
        ((parallel-channel-put! ch) val)
        (let ([o (open-output-string)])
          (fprintf o "[ch-put!] provided ch ~a is not a parallel-channel record\n" ch)
          (raise-error (get-output-string o))
          (values))))

  ;PUBLIC API
  ;  (ch-empty? ch) -> boolean
  (define (ch-empty? ch)
    (if (parallel-channel? ch)
        ((parallel-channel-empty? ch))
        (let ([o (open-output-string)])
          (fprintf o "[ch-empty?] provided ch ~a is not a parallel-channel record\n" ch)
          (raise-error (get-output-string o))
          (values))))


  ;PUBLIC API
  ;  (parallel tb) -> list-of-results
  ;execute tasks in task-box in parallel
  (define (parallel tb)
    (if (task-box? tb)
        ;temporarily assign new values for global variables
        (fluid-let 
          ([*CHANNEL-BLOCK-FLAG* #f]
           [*CHANNEL-BLOCK-ID* 0]
           [*CHANNEL-NON-EMPTY-FLAG* #f]
           [*CHANNEL-NON-EMPTY-IDS* (list)]
           [incrementor (let ([id 0]) 
                          (lambda () (let ([ret id]) 
                                       (set! id (+ id 1)) 
                                       ret)))]
           [append-non-empty-channel-id! 
             (lambda (val)
               (set-unsafe!)
               (printf "*CHANNEL-NON-EMPTY-IDS* PARALLEL 1: ~a\n" *CHANNEL-NON-EMPTY-IDS*)
               (flush-output-port)
               (set! *CHANNEL-NON-EMPTY-IDS* (append *CHANNEL-NON-EMPTY-IDS* (list val)))
               (printf "*CHANNEL-NON-EMPTY-IDS* PARALLEL 2: ~a\n" *CHANNEL-NON-EMPTY-IDS*)
               (flush-output-port)
               (set! *CHANNEL-NON-EMPTY-FLAG* #t)
               (set-safe!))]
           )

          ;enqueue a task that's blocked waiting for input from a 
          ;parallel-channel
          (define (enqueue-waiting-task! task)
            (printf "tb waiting 1: ~a\n" (task-box-waiting tb))
            (flush-output-port)
            (task-box-waiting-set! tb (append (task-box-waiting tb) (list (list *CHANNEL-BLOCK-ID* task))))
            (printf "tb waiting 2: ~a\n" (task-box-waiting tb))
            (flush-output-port)
            (set-block! 0 #f))

          ;execute any tasks waiting for now non-empty parallel-channel id, 
          ;return any tasks still waiting
          (define (enqueue-tasks-waiting-for-id! id)
            (define (loop waiting parsed)
              (if (null? waiting)
                  parsed 
                  (let ([cur (car waiting)])
                    (if (eqv? id (car cur))
                        (let ()
                          (enqueue-task! tb (cadr cur))
                          (loop (cdr waiting) parsed))
                        (loop (cdr waiting) (append parsed (car waiting)))))))
            (let ([parsed (loop (task-box-waiting tb) '())])
              (task-box-waiting-set! tb '())
              parsed))

          ;handle all now non-empty channel ids
          (define (enqueue-waiting-tasks!)
            (map enqueue-tasks-waiting-for-id! *CHANNEL-NON-EMPTY-IDS*)
            (set! *CHANNEL-NON-EMPTY-FLAG* #f)
            (set! *CHANNEL-NON-EMPTY-IDS* '()))

          ;execute tasks in parallel
          (define (exec-tasks tb results)
            ;if channels have new values, enqueue waiting tasks for that channel 
            ;id 
            (when *CHANNEL-NON-EMPTY-FLAG* (enqueue-waiting-tasks!))
            (if (null? (unbox (task-box-contents tb)))
                (if (null? (task-box-waiting tb))
                    results
                    (let ()
                      (sleep (make-time 'time-duration 10000000 0))
                      (exec-tasks tb results))) 
                (let ([task (dequeue-task! tb)])
                  (let ([fuel (task-fuel task)])
                    (let ([engine-ret ((task-engine task)
                                       fuel
                                       (lambda (fuel ret-vals) (list #t ret-vals))
                                       (lambda (new-engine) (list #f new-engine)))])
                      (if (car engine-ret)
                          (exec-tasks tb (append results (list (cadr engine-ret))))
                          (let ()
                            (if (unsafe?) 
                                ;if execution is set to unsafe mode continue execution
                                ;linearly until safe to execute in parallel
                                (let ()
                                  (enqueue-task-as-head! tb (make-task (cadr engine-ret) fuel #t))
                                  (exec-tasks tb results))
                                (let ()
                                  ;if task is blocked on a parallel-channel get!
                                  (if *CHANNEL-BLOCK-FLAG* 
                                      (enqueue-waiting-task! (make-task (cadr engine-ret) fuel))
                                      (enqueue-task! tb (make-task (cadr engine-ret) fuel)))
                                  (exec-tasks tb results))))))))))

          ; exec section (defines are complete) 
          (task-box-running-set! tb #t)
          (let ([ret (exec-tasks tb '())])
            (task-box-running-set! tb #f)
            ret))
        (let ([o (open-output-string)])
          (fprintf o "[parallel] provided task-box argument ~a is not a task-box record\n" tb)
          (raise-error (get-output-string o)))))

  (define *default-task-box* (make-task-box '()))

  ;PUBLIC API
  ;  (go thunk) -> void
  ;  (go thunk fuel) -> void
  ;enqueue a thunk to be executed in a background taskbox
  (define go
    (case-lambda
      [(thunk) (enqueue-task! *default-task-box* (make-task thunk))]
      [(thunk fuel) (enqueue-task! *default-task-box* (make-task thunk fuel))]))

  ;PUBLIC API
  ;  (start) -> list-of-task-results
  ;This must be called before (go) calls will begin executing 
  ;
  ;Example Usage:
  ;  (go thunk1)
  ;  (go thunk2)
  ;  (start) ;(go) calls made by thunk1 or thunk2 will also execute asynchronously
  (define (start)
    (if (not (task-box-running *default-task-box*))
        (parallel *default-task-box*)
        '()))) 
