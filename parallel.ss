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
    ;When this mode is active it forces (parallel) execute linearly (disables 
    ;asynchronous execution). This makes modification to shared/global values 
    ;between tasks safe. parallel-channels use these internally.
    set-unsafe!

    ;  (set-safe!) -> void
    ;Enable parallel execution in (parallel)
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

    ;  (parallel-channel-empty? parallel-channel) -> (->) -> boolean 
    ;  Returns a function that returns #t if values are waiting in the channel 
    ;  otherwise returns false.
    parallel-channel-empty? 

    ;  (parallel-channel-put! ch any) -> (-> any) -> void
    ;  Returns a function that will accept a value and place it into the 
    ;  parallel-channel.
    parallel-channel-put! 

    ;  (parallel-channel-get! ch) -> (->) -> any
    ;  Returns a function that will return a value from the parallel-channel.
    parallel-channel-get! ;blocks till data available

    ;  (parallel task-box) -> list-of-task-results 
    ;execute all tasks contained in given task-box asynchronously before 
    ;returning all task return values as a list in the order completed. 
    ;
    ;Will not return if any tasks are blocking via parallel-channel-get!
    parallel  

    ;  (go task-or-thunk) -> void
    ;Enqueues a thunk or task record to be executed and starts parallel if 
    ;it is not already running. 
    ;
    ;(go) is special because it uses a global task box managed in the
    ;background. This is the easiest way to run tasks asynchronously. As a note,
    ;if parallel is *not* running, this function will block until parallel
    ;returns. However, any (go) function executed by a running task will *NOT*
    ;block, therefore this is safe to use within any task started by (go).
    go) 
  (import (chezscheme)) 

  (define *default-engine-fuel* 50)
  (define *default-task-box* (make-task-box '()))

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
              (new (box task-list) #f)
              (let ()
                (fprintf (current-error-port) 
                         "[error make-task-box] provided task ~a is not a task record, in task list: ~a\n" 
                         (cadr res) 
                         task-list)
                (new (box '()) #f))))))))


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

  (define (enqueue-task-as-head! task-box task)
    (if (task? task)
      (set-box! task-box (append (list task) (task-box-contents (unbox task-box))))
      (fprintf (current-error-port) 
               "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)))

  ;global task timeout 'mutex'
  (define *UNSAFE-TIMEOUT* #f)
  (define (set-unsafe!) (set! *UNSAFE-TIMEOUT* #t))
  (define (set-safe!) (set! *UNSAFE-TIMEOUT* #f))
  (define (unsafe?) *UNSAFE-TIMEOUT*)


  ;global placeholders
  (define *CHANNEL-BLOCK-FLAG* #f)
  (define *CHANNEL-BLOCK-ID* 0)
  (define *CHANNEL-NON-EMPTY-FLAG* #f)
  (define *CHANNEL-NON-EMPTY-IDS* '())

  (define (set-block! val bool)
    (set-unsafe!)
    (set! *CHANNEL-BLOCK-ID* val)
    (set! *CHANNEL-BLOCK-FLAG* bool)
    (set-safe!))

  (define (append-non-empty-channel-id! val)
    (set-unsafe!)
    (set! *CHANNEL-NON-EMPTY-IDS* (append *CHANNEL-NON-EMPTY-IDS* (list val)))
    (set! *CHANNEL-NON-EMPTY-FLAG* #t)
    (set-safe))

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
        [(data get! put! empty?) ;custom parallel-channel case
         (lambda (data get! put! empty?)
           (new 
             data 
             id 
             (lambda ()
               (define (loop)
                 (if (empty? data)
                   (let ()
                     (set-block! id #t)
                     (engine-block)
                     (loop))
                   (get! data)))
               (loop))
             (lambda (val)
               (put! data val)
               (append-non-empty-channel-id! id))
             empty?))]
        [() ;default parallel-channel case
         (lambda ()
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
                           (set! data (cdr data))
                           ret)))
                     (loop))
                   (lambda (val) ;default put!
                     (set! data (append data (list val)))
                     (append-non-empty-channel-id! id))
                   (lambda () (null? data))))))]))) ;default empty?


  ;execute tasks in task-box in parallel
  (define (parallel tb)
    (if (task-box? tb)
      ;temporarily assign new values for global variables
      (fluid-let 
        ([*CHANNEL-BLOCK-FLAG* #f]
         [*CHANNEL-BLOCK-ID* 0]
         [*CHANNEL-NON-EMPTY-FLAG* #f]
         [*CHANNEL-NON-EMPTY-IDS* 0])

        ;enqueue a task that's blocked waiting for input from a 
        ;parallel-channel
        (define (enqueue-waiting-task! task)
          (task-box-waiting-set! (append (task-box-waiting tb) (list *CHANNEL-BLOCK-ID* task)))
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
          (task-box-waiting-set! (loop (task-box-waiting tb))))

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
          (if (null? (unbox (tax-box-contents tb)))
            (if (null? (tax-box-waiting tb))
              results
              (let ()
                (sleep (make-time 10000000))
                (exec-tasks tb results)))
            (let ([task (dequeue-task! tb)])
              (let ([fuel (task-fuel task)])
                (let ([engine-ret ((task-engine task)
                                   fuel
                                   (lambda (fuel ret-vals) '(#t ret-vals))
                                   (lambda (new-engine) '(#f new-engine)))])
                  (if (car engine-ret)
                    (exec-tasks tb (append results (list (cadr engine-ret))))
                    (let ()
                      (if (unsafe?) 
                        ;if execution is set to unsafe mode continue execution
                        ;linearly until safe to execute in parallel
                        (let ()
                          (enqueue-task-as-head! (make-task (cadr engine-ret) fuel #t))
                          (exec-tasks tb results))
                        (let ()
                          ;if task is blocked on a parallel-channel get!
                          (if *CHANNEL-BLOCK-FLAG*
                            (enqueue-waiting-task! (make-task (cadr engine-ret) fuel))
                            (enqueue-task! tb (make-task (cadr engine-ret) fuel #t)))
                          (exec-tasks tb results))))))))))

        (task-box-running-set! tb #t)
        (let ([ret (exec-tasks tb '())])
          (task-box-running-set! tb #f)
          ret))
      (let ()
        (fprintf 
          (current-error-port) 
          "[error: parallel] provided task-box argument ~a is not a task-box record\n" 
          tb))))

  (define go
    (let ()
      (define (intern task)
        (enqueue-task! *default-task-box* (make-task thunk))
        (when (not (task-box-running *default-task-box*))
          (parallel *default-task-box*)))
      (case-lambda
        [(thunk) (intern (make-task thunk))]
        [(thunk-fuel) (intern (make-task thunk fuel))]))))
