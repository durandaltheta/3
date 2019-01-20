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
    ;Enqueues a thunk to be executed.
    ;
    ;As a note these tasks will not be executed unless (parallel) is called 
    ;with the task-box passed to (go). However, any (go) function executed by a 
    ;running task will *NOT* block, therefore this is safe to use (go) within 
    ;tasks run by (parallel).
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
    ;make a channel capable of communicating between asynchronous tasks 
    ;executed by (parallel) or (managed-parallel).
    ;
    ;These channels are *ONLY* safe to use within tasks running in (parallel).
    ;
    ;
    ;  (make-parrallel-channel communication-object blocking-get! put! empty?)
    ;It is possible to create a custom  parallel channel by passing this 
    ;function an arbitrary communication object (a queue, channel, pipe, etc..) 
    ;and the following object manipulation functions:
    ;  blocking-get! : returns a value from the object. Blocks if nothing in 
    ;                  the object.
    ;  put! : puts a value into the object. If this blocks, it will block
    ;         the entire thread (including parallel/managed-parallel)
    ;  empty? : returns #t if object is empty, else #f
    ;
    ;A custom parallel-channel is useful if gathering input from outside the 
    ;process or thread is necessary
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

    ;test bindings
    parallel-debug
    dprint)
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


  ;global task timeout 'mutex'
  (define (set-unsafe! tb) 
    (let ([hash (task-box-state-hash tb)])
      (hashtable-set! hash 'unsafe-timeout #t)))

  (define (set-safe! tb) 
    (let ([hash (task-box-state-hash tb)])
      (hashtable-set! hash 'unsafe-timeout #f)))

  (define (unsafe? tb)
    (let ([hash (task-box-state-hash tb)])
      (hashtable-ref hash 'unsafe-timeout #f)))


  (define (safe-get-thread-id)
    (if (threaded?) (get-thread-id) 0))


  (define-record-type
    task-box 
    (fields (immutable thread-id)
            (mutable contents)
            (mutable waiting)
            (mutable running)
            (mutable pause)
            (mutable unsafe-timeout)
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
          (new (safe-get-thread-id) (list) (list) #f #f #f #f 0 #f (list))))))


  (define (enqueue-task! tb task)
    (if (task? task)
      (set-box! (task-box-contents tb) (append (task-box-contents tb) (list task)))
      (let ([o (open-output-string)])
        (fprintf o "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)
        (raise-error (get-output-string o)))))


  (define (dequeue-task! tb) ;null? checking happens in (parallel)
    (let ([tasks (task-box-contents tb)])
      (let ([task (car tasks)])
        (set-box! (task-box-contents tb) (cdr tasks))
        task)))


  (define (enqueue-task-as-head! tb task)
    (if (task? task)
      (set-box! (task-box-contents tb) (append (list task) (task-box-contents tb)))
      (let ([o (open-output-string)])
        (fprintf o "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)
        (raise-error (get-output-string o)))))


  (define (set-get-block! tb id bool)
    (set-unsafe! tb)
    (task-box-channel-block-id-set! tb tb)
    (task-box-channel-get-block-flag-set! bool)
    (set-safe! tb))


  (define (set-put-block! tb id bool)
    (set-unsafe! tb)
    (task-box-channel-block-id-set! tb tb)
    (task-box-channel-put-block-flag-set! bool)
    (set-safe! tb))


  (define append-non-empty-channel-id! 
    (lambda (tb val)
      (let ([cur-non-empty-ids (task-box-channel-non-empty-ids tb)])
        (set-unsafe! tb)
        (when (not (member val cur-non-empty-ids))
          (task-box-channel-non-empty-ids-set! tb (append cur-non-empty-ids (list val))))
        (task-box-channel-non-empty-flag-set! tb #t)
        (set-safe! tb))))


  (define append-non-full-channel-id! 
    (lambda (tb val)
      (let ([cur-non-full-ids (task-box-channel-non-full-ids tb)])
        (set-unsafe! tb)
        (when (not (member val cur-non-full-ids))
          (task-box-channel-non-full-ids-set! tb (append cur-non-full-ids (list val))))
        (task-box-channel-non-full-flag-set! tb #t)
        (set-safe! tb)))) 


  (define (pause-parallel tb)
    (define (wait-for-pause)
      (when (task-box-running tb)
        (let ()
          (sleep (make-time 'time-duration 10000000 0))
          (wait-for-pause))))
    (task-box-pause-set! tb #t)
    (wait-for-pause))


  (define (unpause-parallel tb)
    (task-box-pause-set! tb #f))


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
                                      (append-non-full-channel-id! tb id)
                                      (get! obj))))]
                 [internal-put! (lambda (val)
                                  (if (full? obj)
                                    failure
                                    (let ()
                                      (append-non-empty-channel-id! tb id)
                                      (put! obj val))))])
             (new tb
                  obj 
                  id 
                  (lambda () ;custom get!
                    (define (loop)
                      (if (empty? obj)
                        (let ()
                          (set-block! tb id #t 'unknown)
                          (engine-block)
                          (loop))
                        (internal-get!)))
                    (loop))
                  intern-put! ; custom put!
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
           (let ([obj (list)]
                 [id (incrementor)]
                 [failure #f]
                 [default-get! (lambda ()
                                 (let ([ret (car obj)])
                                   (set! obj (cdr obj))
                                   ret))]
                 [default-put! (lambda () (let ([ret (car obj)])
                                            (set! obj (cdr obj))
                                            ret))])
             (let ([internal-get! (lambda ()
                                    (if (empty? obj)
                                      failure
                                      (let ()
                                        (append-non-full-channel-id! tb id)
                                        (default-get! obj))))]
                   [internal-put! (lambda (val)
                                    (if (full? obj)
                                      failure
                                      (let ()
                                        (append-non-empty-channel-id! tb id)
                                        (default-put! obj val))))])
               (new tb
                    obj
                    id
                    (lambda () ;default get!
                      (define (loop)
                        (if (empty? obj)
                          (let ()
                            (set-block! tb id #t 'unknown)
                            (engine-block)
                            (loop))
                          (internal-get!)))
                      (loop))
                    (lambda (val) ;default put!
                      (define (loop)
                        (if (full? obj)
                          (let ()
                            (set-block! tb id 'unknown #t)
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
                      (unpause-parallel tb)))))])))) ;default external-put!


  ;for use in ch-get! and ch-put!. Returns whether a given channel is being 
  ;used within a task-box running in parallel within the same process and 
  ;thread
  (define (in-running-task? tb)
    (if (eqv? (task-box-process-id tb) (get-process-id))
      (if (eqv? (task-box-thread-id tb) (safe-get-thread-id))
        (if (eqv? (task-box-running tb) #t)
          #t)
        #f)
      #f))

  ;PUBLIC API
  ;  (ch-get! ch) -> any
  (define (ch-get! ch)
    (if (parallel-channel? ch)
      (let ([tb (parallel-channel-task-box ch)])
        (if (in-running-task? tb)
          ((parallel-channel-get! ch))
          ((parallel-channel-external-get! ch))))
      (let ([o (open-output-string)])
        (fprintf o "[ch-get!] provided ch ~a is not a parallel-channel record\n" ch)
        (raise-error (get-output-string o))
        (values))))

  ;PUBLIC API
  ;  (ch-put! ch val) -> void
  (define (ch-put! ch val)
    (if (parallel-channel? ch)
      (let ([tb (parallel-channel-task-box ch)])
        (if (in-running-task? tb)
          ((parallel-channel-put! ch) val)
          ((parallel-channel-external-put! ch) val)))
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
  ;  (ch-empty? ch) -> boolean
  (define (ch-full? ch)
    (if (parallel-channel? ch)
      ((parallel-channel-full? ch))
      (let ([o (open-output-string)])
        (fprintf o "[ch-empty?] provided ch ~a is not a parallel-channel record\n" ch)
        (raise-error (get-output-string o))
        (values))))


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
        (define (enqueue-waiting-task! id task)
          (task-box-waiting-set! tb (append (task-box-waiting tb) (list (list id task))))
          (set-block! tb 0 #f 'unknown))

        ;execute any tasks waiting for now non-empty parallel-channel id, 
        ;return any tasks still waiting
        (define (enqueue-tasks-waiting-for-id! id)
          (define (loop waiting parsed)
            (if (null? waiting)
              parsed 
              (let ([cur (car waiting)])
                (if (eqv? id (car cur))
                  (let ()
                    (dprint tb (list "enqueueing cur: " cur))
                    (enqueue-task! tb (cadr cur))
                    (loop (cdr waiting) parsed))
                  (loop (cdr waiting) (append parsed (car waiting)))))))
          (dprint tb (list "waiting: " (task-box-waiting tb)))
          (let ([parsed (loop (task-box-waiting tb) '())])
            (task-box-waiting-set! tb '())
            parsed))

        ;handle all now non-empty channel ids
        (define (enqueue-no-longer-waiting-tasks!)
          (let ([hash (task-box-state-hash tb)])
            (dprint tb (list "enqueue-no-longer-waiting-tasks! 1"))
            (map enqueue-tasks-waiting-for-id! (hashtable-ref hash 'channel-non-empty-ids (list)))
            (dprint tb (list "enqueue-no-longer-waiting-tasks! 2"))
            (hashtable-set! hash 'channel-non-empty-flag #f)
            (hashtable-set! 'channel-non-empty-ids (list))))

        (define (check-for-pause)
          (define (wait-loop)
            (when (task-box-pause tb)
              (let ()
                (sleep (make-time 'time-duration 10000000 0))
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
          (let ([hash (task-box-state-hash tb)])
            ;if channels have new values, enqueue waiting tasks for that channel 
            ;id  
            (when (hashtable-ref hash 'channel-non-empty-flag #f) (enqueue-no-longer-waiting-tasks!))
            (if (null? (task-box-contents tb))
              (if (eqv? '() (task-box-waiting tb))
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
                        (if (unsafe? tb) 
                          ;if execution is set to unsafe mode continue execution
                          ;linearly until safe to execute in parallel
                          (let ([new-task (make-task (cadr engine-ret) fuel #t)])
                            (dprint tb (list "exec-tasks unsafe new task: " new-task))
                            (enqueue-task-as-head! tb new-task)
                            (exec-tasks tb results))
                          (let ([new-task (make-task (cadr engine-ret) fuel #t)])
                            ;if task is blocked on a parallel-channel get!
                            (if (hashtable-ref hash 'channel-get-block-flag #f)
                              (enqueue-waiting-task! (hashtable-ref hash 'channel-block-id 0) new-task)
                              (enqueue-task! tb new-task))
                            (exec-tasks tb results)))))))))))

        ; exec section (defines are complete) 
        (task-box-running-set! tb #t)
        (let ([ret (exec-tasks tb '())])
          (task-box-running-set! tb #f)
          ret))
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
