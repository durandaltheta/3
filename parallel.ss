(library 
  (parallel)
  (export 
    ;  (make-task-box list-of-tasks) -> task-box 
    ;create a box of tasks for parallel execution from a list of task records
    make-task-box 

    ;  (task-box? a) -> boolean
    ;returns #t if argument a is a task-box, else returns #f
    task-box?

    ;  (go task-box thunk) -> void
    ;  (go task-box thunk fuel) -> void
    ;Enqueues a thunk into a task-box in order to be executed by (parallel). 
    ;If (go) was invoked by a thunk currently running in (parallel), the newly
    ;enqueued thunk will also be asynchronously executed.
    go

    ;  (set-safe!) -> void
    ;Enable parallel execution in (parallel). This is #t by default and only 
    ;set #f when (set-unsafe!) is called. 
    set-safe!

    ;  (set-unsafe!) -> void
    ;When this mode is active it forces (parallel) to execute linearly 
    ;(disables asynchronous execution). This makes modification to 
    ;shared/global values safe. parallel-channels use these internally.
    ;
    ;A common example of modifying a shared value is writing to 
    ;(current-output-port) with (display) or (printf). Thus to ensure a correct
    ;arbitrary print:
    ;(set-unsafe!)
    ;(printf "foo!\n")
    ;(set-safe!)
    set-unsafe!

    ;Returns #t if in unsafe mode, otherwise #f
    unsafe?

    ;  (make-parallel-channel) -> parallel-channel  
    ;make a channel capable of communicating both between asynchronous tasks 
    ;running in (parallel), but also by code in other threads. It is 
    ;recommended when using parallel channel blocking calls to communicate 
    ;between two tasks running in separate (parallel) executions (in different 
    ;threads) to use (unsafe!) before the blocking call and (safe!) when the 
    ;call finishes.
    ;
    ;
    ;  (make-parrallel-channel communication-object blocking-get! put! empty? failure-value)
    ;It is possible to create a custom  parallel channel by passing this 
    ;function an arbitrary communication object (a queue, channel, pipe, etc..) 
    ;and the following object manipulation functions:
    ;  get! : returns a value from the object.
    ;  put! : puts a value into the object. 
    ;  empty? : returns #t if object is empty, else #f
    ;  full? : returns #t if object is full, else #f
    ;
    ;Also must pass in a failure-value. This is returned if unable to complete
    ;a pch-try-get!/pch-try-put! operation.
    ;
    ;If input get!/put! functions block, care must be taken in any code that 
    ;uses the raw communication object instead of the parallel-channel
    ;encapsulation provided here. Essentially, don't remove values from a 
    ;channel when a task running in (parallel) is expected to also remove 
    ;values from via a parallel-channel. The same is true of adding values, 
    ;don't add values to a communication object where a task running in 
    ;(parallel) is also expected to add values to the object via a 
    ;parallel-channel. Otherwise edgecase errors could occure where 
    ;unexpectedly removed or added values could cause the entire (parallel) 
    ;thread to block until values are added or removed from the communication
    ;object.
    ;
    ;Similarly, using a custom parallel-channel is useful when gathering input 
    ;from outside the thread is necessary. If the using a custom 
    ;parallel-channel to get data from outside the process, beware that 
    ;pch-get!/pch-put! may use busy waiting to block for data rather than a 
    ;the more efficient algorithm used by default parallel-channels.
    make-parallel-channel 

    ;  (parallel-channel? a) -> boolean
    ;returns #t if argument a is a parallel-channel, else returns #f
    parallel-channel?

    ;  (ch-get! parallel-channel) -> any
    ;Get value from parallel channel, blocking until available.
    pch-get!

    ;  (ch-try-get! parallel-channel) -> any
    ;Try to get a value from parallel channel, returning failure value 
    ;otherwise.
    pch-try-get!

    ;  (ch-put! parallel-channel any) -> void
    ;Put a value into a parallel-channel, blocking until space available.
    pch-put!

    ;  (ch-try-put! parallel-channel any) -> void
    ;Try to put a value into a parallel-channel, returning failure value 
    ;otherwise. 
    pch-try-put!

    ;  (pch-empty? parallel-channel) -> boolean
    ;Returns #t if channel is empty, else returns #f
    pch-empty?


    ;  (pch-full? parallel-channel) -> boolean
    ;Returns #t if channel is full, else returns #f
    pch-full?

    ;  (parallel task-box) -> list-of-task-results 
    ;execute all tasks contained in given task-box asynchronously before 
    ;returning all task return values as a list in the order completed. 
    ;
    ;WARNINGS:
    ;- Will not return if any tasks are blocking via parallel-channel-get!/put!
    ;- Do *not* use (set-timer) inside a task, will break implementation
    ;- To make modification of shared memory safe, invoke (set-unsafe!). This 
    ;  forces parallel to execute linearly. Use (set-safe!) to restore normal
    ;  asynchronous execution.
    parallel  

    ;test bindings
    parallel-debug
    dprint)
  (import (chezscheme)) 

  ; convenience error message
  (define (raise-error msg)
    (raise
      (condition
        (make-error)
        (make-message-condition msg))))

  ; global hidden defaults
  (define *default-engine-fuel* 50) 
  (define *default-sleep-nanoseconds* 10000000) ;10 milliseconds
  (define *default-sleep-time* (make-time 'time-duration *default-sleep-nanoseconds* 0))
  (define (default-sleep) (sleep *default-sleep-time*))

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


  ; task timeout 'mutex' functions
  (define (set-unsafe! tb) (task-box-unsafe-timeout-set! tb #t))
  (define (set-safe! tb) (task-box-unsafe-timeout-set! tb #f))
  (define (unsafe? tb) (task-box-unsafe-timeout tb))

  ; get a thread id whether or not this is running in threaded chezscheme
  (define (safe-get-thread-id) (if (threaded?) (get-thread-id) 0))

  (define-record-type
    task-box 
    (fields (mutable thread-id)
            (mutable task-queue)
            (mutable waiting-gets)
            (mutable waiting-puts)
            (mutable running)
            (mutable pause)
            (mutable num-running-tasks)
            (mutable num-custom-get)
            (mutable num-custom-put)
            (mutable channel-get-block-flag) ;indicates current task is blocked on a get!
            (mutable channel-put-block-flag) ;indicates current task is blocked on a put!
            (mutable channel-block-id) ;id task is blocked waiting for
            (mutable channel-non-empty-flag) ;indicates 1 or more channels have new data
            (mutable channel-non-empty-ids) ;list of channels with new data
            (mutable channel-non-full-flag) ;indicates 1 or more channels are no longer full
            (mutable channel-non-full-ids)) ;list of channels that are no longer full
    (protocol 
      (lambda (new) 
        (lambda ()
          (new (safe-get-thread-id) ;thread-id
               (list) ;task-queue
               (list) ;waiting-gets
               (list) ;waiting-puts
               #f ;running
               #f ;pause
               0 ;num-running-tasks
               0 ;num-custom-get
               0 ;num-custom-put
               #f ;channel-get-block-flag
               #f ;channel-put-block-flag
               0 ;channel-block-id
               #f ;channel-non-empty-flag
               (list) ;channel-non-empty-ids
               #f ;channel-non-full-flag
               (list)))))) ;channel-non-full-ids


  ; enqueue the new task at the end of the task queue
  (define (enqueue-task! tb task)
    (if (task? task)
      (let ([num-running (task-box-num-running-tasks tb)])
        (task-box-num-running-tasks-set! tb (+ num-running 1))
        (set-box! (task-box-task-queue tb) (append (task-box-task-queue tb) (list task))))
      (let ([o (open-output-string)])
        (fprintf o "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)
        (raise-error (get-output-string o)))))


  ; remove a task from the beginning of the task queue
  (define (dequeue-task! tb) ;null? checking happens in (parallel)
    (let ([tasks (task-box-task-queue tb)])
      (let ([task (car tasks)])
        (set-box! (task-box-task-queue tb) (cdr tasks))
        task)))


  ; enqueue the new task at the beginning of the task queue
  (define (enqueue-task-as-head! tb task)
    (if (task? task)
      (set-box! (task-box-task-queue tb) (append (list task) (task-box-task-queue tb)))
      (let ([o (open-output-string)])
        (fprintf o "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)
        (raise-error (get-output-string o)))))


  ; indicate to running (parallel) environment the current task is blocked on a
  ; get! call
  (define (set-get-block! tb id)
    (set-unsafe! tb)
    (task-box-channel-block-id-set! tb id)
    (task-box-channel-get-block-flag-set! #t)
    (set-safe! tb))


  ; indicate to running (parallel) environment the current task is blocked on a
  ; put! call
  (define (set-put-block! tb id)
    (set-unsafe! tb)
    (task-box-channel-block-id-set! tb id)
    (task-box-channel-put-block-flag-set! #t)
    (set-safe! tb))


  ; append a channel id to the list of ids that are not empty
  (define append-non-empty-channel-id! 
    (lambda (tb val)
      (let ([cur-non-empty-ids (task-box-channel-non-empty-ids tb)])
        (when (not (member val cur-non-empty-ids))
          (task-box-channel-non-empty-ids-set! tb (append cur-non-empty-ids (list val))))
        (task-box-channel-non-empty-flag-set! tb #t))))


  ; append a channel id to the list of ids that are not full
  (define append-non-full-channel-id! 
    (lambda (tb val)
      (let ([cur-non-full-ids (task-box-channel-non-full-ids tb)])
        (when (not (member val cur-non-full-ids))
          (task-box-channel-non-full-ids-set! tb (append cur-non-full-ids (list val))))
        (task-box-channel-non-full-flag-set! tb #t)))) 


  ; pause execution of (parallel)
  (define (pause-parallel tb)
    (define (wait-for-pause)
      (when (task-box-running tb)
        (let ()
          (default-sleep)
          (wait-for-pause))))
    (task-box-pause-set! tb #t)
    (wait-for-pause))


  ; unpause execution of (parallel)
  (define (unpause-parallel tb)
    (task-box-pause-set! tb #f))


  ; create an incrementor function that always returns (+ prev-return-val 1) 
  ; starting at 0
  (define get-incrementor (lambda ()
                            (let ([id 0]) 
                              (lambda () (let ([ret id]) 
                                           (set! id (+ id 1)) 
                                           ret)))))

  ;function that always emits a unique numerical value per invocation
  (define incrementor (get-incrementor))

  ;record that contains threadsafe data passing functions. Can be created 
  ;without arguments to create a default record OR with custom communication 
  ;object and functions get!, put!, empty? and full? and return value failure 
  ;(for when the object is empty or full) that operate on said communication 
  ;object.
  (define-record-type
    parallel-channel
    (fields 
      (mutable task-box)
      (mutable obj)
      (immutable id)
      (immutable failure)
      (immutable get!)
      (immutable put!)
      (immutable empty?)
      (immutable full?)
      ;for use in code that is not running in parallel (ex: another thread or 
      ;process). If using a default parallel-channel then if channel is empty 
      ;returns #f.
      (immutable external-get!)  
      ;for use in code that is not running in parallel (ex: another thread or 
      ;process)     
      (immutable external-put!)) 
    (protocol
      (lambda (new)
        (case-lambda 
          [(tb obj get! put! empty? full? failure) ;custom parallel-channel case 
           (let ([id (incrementor)]
                 [internal-get! (lambda ()
                                  (if (empty? obj)
                                    failure
                                    (let ()
                                      (set-unsafe!)
                                      (append-non-full-channel-id! tb id)
                                      (get! obj)
                                      (set-safe!))))]
                 [internal-put! (lambda (val)
                                  (if (full? obj)
                                    failure
                                    (let ()
                                      (set-unsafe!)
                                      (append-non-empty-channel-id! tb id)
                                      (put! obj val)
                                      (set-safe!))))])
             (new tb
                  obj 
                  id 
                  failure
                  ;custom get!: less efficient blocking behavior than normal 
                  ;get!. If the only tasks that are running are tasks waiting
                  ;for a custom get! it sleeps for a brief period before 
                  ;calling (engine-block), otherwise it just calls 
                  ;(engine-block) to allow other tasks to run.
                  (lambda () 
                    (define (loop waiting)
                      (set-unsafe! tb)
                      (if (empty? obj)
                        (let ([num-waiting (task-box-num-custom-get tb)])
                          (set-safe!)
                          (when (not waiting) (task-box-num-custom-get-set! tb (+ num-waiting 1)))
                          (when (eqv? (task-box-num-running-tasks tb)
                                      num-waiting)
                            (default-sleep))
                          (engine-block)
                          (loop #t))
                        (let ([res (internal-get!)])
                          (set-safe!)
                          (if (eqv? res failure)
                            (let ()
                              (engine-block)
                              (loop #t))
                            (let ()
                              (task-box-num-custom-get-set! tb (- num-waiting 1))
                              res)))))
                    (loop #f))
                  ;custom put!
                  (lambda () 
                    (define (loop waiting)
                      (set-unsafe! tb)
                      (if (empty? obj)
                        (let ([num-waiting (task-box-num-custom-put tb)])
                          (set-safe!)
                          (when (not waiting) (task-box-num-custom-put-set! tb (+ num-waiting 1)))
                          (when (eqv? (task-box-num-running-tasks tb)
                                      num-waiting)
                            (default-sleep))
                          (engine-block)
                          (loop #t))
                        (let ([res (internal-get!)])
                          (set-safe!)
                          (if (eqv? res failure)
                            (let ()
                              (engine-block)
                              (loop #t))
                            (let ()
                              (task-box-num-custom-put-set! tb (- num-waiting 1))
                              res)))))
                    (loop #f))
                  empty? ;custom empty?
                  full? ;custom full?
                  (lambda () ;custom extern-get!
                    (define ret failure)
                    (pause-parallel tb)
                    (set! ret (internal-get!))
                    (unpause-parallel tb))
                  (lambda (val) ;custom extern-get!
                    (pause-parallel tb)
                    (internal-put! val)
                    (unpause-parallel tb))))] ;custom extern-put!
          [(tb) ;default parallel-channel case
           (let* ([obj (list)]
                  [id (incrementor)]
                  [failure #f]
                  [default-get! (lambda ()
                                  (let ([ret (car obj)])
                                    (set! obj (cdr obj))
                                    ret))]
                  [default-put! (lambda () (let ([ret (car obj)])
                                             (set! obj (cdr obj))
                                             ret))]
                  [internal-get! (lambda ()
                                   (if (empty? obj)
                                     failure
                                     (let ()
                                       (set-unsafe!)
                                       (append-non-full-channel-id! tb id)
                                       (default-get! obj)
                                       (set-safe!))))]
                  [internal-put! (lambda (val)
                                   (if (full? obj)
                                     failure
                                     (let ()
                                       (set-unsafe!)
                                       (append-non-empty-channel-id! tb id)
                                       (default-put! obj val)
                                       (set-safe!))))])
             (new tb
                  obj
                  id
                  failure
                  (lambda () ;default get!
                    (define (loop)
                      (if (empty? obj)
                        (let ()
                          (set-get-block! tb id)
                          (engine-block)
                          (loop))
                        (internal-get!)))
                    (loop))
                  (lambda (val) ;default put!
                    (define (loop)
                      (if (full? obj)
                        (let ()
                          (set-put-block! tb id)
                          (engine-block)
                          (loop))
                        (let ()
                          (set! obj (append obj (list val)))
                          (append-non-empty-channel-id! tb id))))
                    (loop))
                  (lambda () (eqv? '() obj)) ;default empty?
                  (lambda () #f) ;default full?
                  (lambda () ;default external-get!
                    (pause-parallel tb)
                    (internal-get!)
                    (unpause-parallel tb))
                  (lambda (val)
                    (pause-parallel tb)
                    (internal-put! val)
                    (unpause-parallel tb))))])))) ;default external-put!


  ;for use in pch-get! and pch-put!. Returns whether a given channel is being 
  ;used within a task-box running in parallel within the same process and 
  ;thread
  (define (in-running-task? tb)
    (if (eqv? (task-box-thread-id tb) (safe-get-thread-id))
      (if (eqv? (task-box-running tb) #t)
        #t
        #f)
      #f))


  (define (error-not-parallel-channel ch)
    (let ([o (open-output-string)])
      (fprintf o "[pch-get!] provided ch ~a is not a parallel-channel record\n" ch)
      (raise-error (get-output-string o))
      (values)))


  ;explaining the implementation of the following parallel-channel manipulation 
  ;functions.
  ;
  ;parallel-channel-get!/put! are used by tasks running in parallel and blocks 
  ;by default. This blocking behavior is managed by (parallel) itself. 
  ;pch-try-get!/put! implement an empty?/full? check inside of unsafe!/safe! 
  ;calls, ensuring the the behavior is atomic.
  ;
  ;Since parallel-channel-external-get!/put! are used by code *not* running in 
  ;(parallel) blocking behavior is not available and thus these functions are 
  ;try-get!/try-put! by default. Thus we implement busy waiting to create 
  ;blocking behavior. Data safety is ensured by pause-parallel/unpause-parallel
  ;calls, which ensure that (parallel) is paused before we attempt to modify 
  ;data.


  ;PUBLIC API
  ;  (pch-get! ch) -> any
  ;blocking get
  (define (pch-get! ch)
    (if (parallel-channel? ch)
      (let ([tb (parallel-channel-task-box ch)])
        (if (in-running-task? tb)
          ;pch-get! from running (parallel) tasks block by default
          ((parallel-channel-get! ch)) 
          ;must implement blocking manually otherwise
          (let ()
            (define (wait-loop)
              (if (pch-empty? ch)
                (let ()
                  (default-sleep)
                  (wait-loop))
                (let ([res ((parallel-channel-external-get! ch))])
                  (if (eqv? res (parallel-channel-failure ch))
                    (wait-loop)
                    res))))
            (wait-loop))))
      (error-not-parallel-channel)))

  ;PUBLIC API
  ;  (pch-try-get! ch) -> any
  ;non-blocking get
  (define (pch-try-get! ch)
    (if (parallel-channel? ch)
      (let ([tb (parallel-channel-task-box ch)])
        (if (in-running-task? tb)
          ;must implement try-get behavior for pch-try-get! calls from within 
          ;tasks running in (parallel)
          (let ()
            (set-unsafe!)
            (if (pch-empty? ch)
              (parallel-channel-failure ch)
              ((parallel-channel-get! ch)))
            (set-safe!))
          ;pch-try-get! calls work without extra logic otherwise
          ((parallel-channel-external-get! ch))))
      (error-not-parallel-channel)))

  ;PUBLIC API
  ;  (pch-put! ch val) -> void
  (define (pch-put! ch val)
    (if (parallel-channel? ch)
      (let ([tb (parallel-channel-task-box ch)])
        (if (in-running-task? tb)
          ((parallel-channel-put! ch) val)
          ((parallel-channel-external-put! ch) val)))
      (error-not-parallel-channel)))

  ;PUBLIC API
  ;  (pch-try-put! ch val) -> void
  (define (pch-try-put! ch val)
    (if (parallel-channel? ch)
      (let ([tb (parallel-channel-task-box ch)])
        (if (in-running-task? tb)
          ((parallel-channel-put! ch) val)
          ((parallel-channel-external-put! ch) val)))
      (error-not-parallel-channel)))

  ;PUBLIC API
  ;  (pch-empty? ch) -> boolean
  (define (pch-empty? ch)
    (if (parallel-channel? ch)
      ((parallel-channel-empty? ch))
      (error-not-parallel-channel)))

  ;PUBLIC API
  ;  (pch-empty? ch) -> boolean
  (define (ch-full? ch)
    (if (parallel-channel? ch)
      ((parallel-channel-full? ch))
      (error-not-parallel-channel)))


  (define parallel-debug #f)
  (define dprint
    (lambda (tb args)
      (when parallel-debug
        (let ()
          (set-unsafe! tb)
          (display "[PARALLEL-DEBUG-PRINT] ")
          (map display args)
          (display "\n")
          (flush-output-port)
          (set-safe! tb)))))

  ;PUBLIC API
  ;  (parallel tb) -> list-of-results
  ;execute tasks in task-box in parallel
  (define (parallel tb)
    (if (task-box? tb)
      (let ()
        ;enqueue a task that's blocked waiting for input from a 
        ;parallel-channel
        (define (enqueue-waiting-get! id task)
          (task-box-waiting-gets-set! tb (append (task-box-waiting-gets tb) (list (list id task))))
          (task-box-channel-get-block-flag-set! #t))

        ;handle all now non-empty channel ids
        (define (enqueue-no-longer-waiting-gets!)
          (map enqueue-tasks-waiting-for-get-id! (task-box-channel-non-empty-ids tb))
          (task-box-channel-non-empty-flag-set! tb #f)
          (task-box-channel-non-empty-ids-set! tb (list)))

        ;execute any tasks waiting for now non-empty parallel-channel id, 
        ;return any tasks still waiting
        (define (enqueue-tasks-waiting-for-get-id! id)
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
        
        ;enqueue a task that's blocked waiting for parallel-channel to be 
        ;non-full
        (define (enqueue-waiting-put! id task)
          (task-box-waiting-puts-set! tb (append (task-box-waiting-puts tb) (list (list id task))))
          (task-box-channel-put-block-flag-set! #f))

        ;execute any tasks waiting for now non-empty parallel-channel id, 
        ;return any tasks still waiting
        (define (enqueue-tasks-waiting-for-put-id! id)
          (define (loop waiting parsed)
            (if (null? waiting)
              parsed 
              (let ([cur (car waiting)])
                (if (eqv? id (car cur))
                  (let ()
                    (enqueue-task! tb (cadr cur))
                    (loop (cdr waiting) parsed))
                  (loop (cdr waiting) (append parsed (car waiting)))))))
          (let ([parsed (loop (task-box-waiting-puts tb) '())])
            (task-box-waiting-set! tb '())
            parsed))

        ;handle all now non-full channel ids
        (define (enqueue-no-longer-waiting-puts!)
          (map enqueue-tasks-waiting-for-put-id! (task-box-channel-non-full-ids tb))
          (task-box-channel-non-full-flag-set! tb #f)
          (task-box-channel-non-full-ids-set! tb (list)))

        ; checks if outside code is requesting a pause in parallel execution 
        ; and wait for unpause.
        (define (check-for-pause)
          (define (wait-loop)
            (when (task-box-pause tb)
              (let ()
                (default-sleep)
                (wait-loop))))
          (when (task-box-pause tb)
            (let ()
              (task-box-running-set! tb #f)
              (wait-loop)
              (task-box-running-set! tb #t))))

        ;execute tasks in parallel
        (define (exec-tasks tb results)
          ;check for pause in parallel execution and handle it
          (check-for-pause)
          ; if channels have new values, enqueue waiting tasks for those
          ; channel ids
          (when (task-box-channel-non-empty-flag tb) 
            (enqueue-no-longer-waiting-gets!))

          ; if channels are no longer full, enqueue waiting tasks for those
          ; channel ids
          (when (task-box-channel-non-full-flag tb) 
            (enqueue-no-longer-waiting-puts!))

          (if (eqv? '() (task-box-task-queue tb))
            ; if there's no remaining tasks
            (if (eqv? '() (task-box-waiting tb))
              ; if there's no waiting tasks, return results
              results
              ; otherwise sleep (waiting on input from outside thread) and 
              ; recurse exec-tasks
              (let ()
                (default-sleep)
                (exec-tasks tb results)))

            ; pull the first task of the queue
            (let ([task (dequeue-task! tb)])
              ; get task fuel value because it's used more than once
              (let ([fuel (task-fuel task)])
                ; execute task engine and handle results
                (let ([engine-ret ((task-engine task)
                                   fuel
                                   (lambda (fuel ret-vals) (list #t ret-vals))
                                   (lambda (new-engine) (list #f new-engine)))])
                  (if (car engine-ret)
                    ; task completed, do cleanup and recurse exec-tasks
                    (let ([num-running (task-box-num-running-tasks tb)])
                      (when (unsafe? tb) (set-safe! tb))
                      (task-box-num-running-tasks-set! tb (- num-running 1))
                      (exec-tasks tb (append results (list (cadr engine-ret)))))
                    (let ()
                      (if (unsafe? tb) 
                        ; if execution is set to unsafe mode continue execution
                        ; linearly until safe to execute in parallel
                        (let ([new-task (make-task (cadr engine-ret) fuel #t)])
                          (dprint tb (list "exec-tasks unsafe new task: " new-task))
                          (enqueue-task-as-head! tb new-task)
                          (exec-tasks tb results))
                        (let ([new-task (make-task (cadr engine-ret) fuel #t)])
                          (if (task-box-channel-get-block-flag tb)
                            ; if task is blocked on a parallel-channel get!
                            (enqueue-waiting-get! (task-box-channel-block-id tb) new-task)
                            (if (task-box-channel-put-block-flag tb)
                              ; else if task is blocked on a parallel-channel put!
                              (enqueue-waiting-put! (task-box-channel-block-id tb) new-task)
                              ; otherwise enqueue task to tail of task list
                              (enqueue-task! tb new-task)))
                          ; recurse exec-tasks
                          (exec-tasks tb results))))))))))

        ; exec section (defines are complete) 
        (task-box-thread-id-set! tb (safe-get-thread-id))
        (task-box-running-set! tb #t)

        ; execute tasks in parallel
        (let ([ret (exec-tasks tb '())])
          (task-box-running-set! tb #f)
          ret))
      ; handle case where provided task-box is not a task-box record
      (let ([o (open-output-string)])
        (fprintf o "[parallel] provided task-box argument ~a is not a task-box record\n" tb)
        (raise-error (get-output-string o)))))

  ;PUBLIC API
  ;  (go thunk) -> void
  ;  (go thunk fuel) -> void
  ;enqueue a thunk to be executed in a background taskbox
  (define go
    (case-lambda
      [(tb thunk) (enqueue-task! tb (make-task thunk))]
      [(tb thunk fuel) (enqueue-task! tb (make-task thunk fuel))]))) 
