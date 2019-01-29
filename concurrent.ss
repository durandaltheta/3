(library 
  (concurrent)
  (export 
    ;  (concurrent concurrent-handler) -> list-of-task-results 
    ;Asynchronously execute all tasks given from concurrent-handler.
    ;concurrent-handler is a function that takes 1 input argument and outputs a
    ;task or other arbitrary value. 
    ;
    ;(concurrent) will execute (concurrent-handler) with the result of the most 
    ;recent task (starting with '() during the first evaluation). The output of 
    ;concurrent-handler should be a task record. (concurrent) will return 
    ;list-of-task-results when concurrent-handler returns anything that is not 
    ;a task record. A result value is only added to list-of-task-results if 
    ;the task engine successfully returned (it did not time out or block).
    concurrent  

    ;  (make-task thunk) -> task
    ;  (make-task thunk fuel) -> task
    ;  (make-task engine-or-thunk fuel is-engine) -> task 
    ;thunk: any function that takes no arguments 
    ;fuel: amount of fuel for process before blocking and allowing other tasks
    ;to evaluate.
    ;engine-or-thunk: An engine object or a thunk function.
    ;is-engine: #t if engine-or-thunk is an engine object, else #f 
    ;
    ;This is typically only used if creating a custom concurrent-handler
    make-task 

    ;  (task? a) -> bool 
    ;Returns #t if given value is a task record, else #f.
    ;
    ;This is typically only used if creating a custom concurrent-handler
    task?

    ;;;------------------------------------------------------------------------
    ;;;The following are convenience functions created using the above 
    ;;;functions. 

    ;  (make-task-box) -> task-box 
    ;create a box of tasks for concurrent execution from a list of task records
    make-task-box 

    ;  (task-box? a) -> boolean
    ;returns #t if argument a is a task-box, else returns #f
    task-box? 

    ;  (get-concurrent-handler task-box) -> handler 
    ;Get the concurrent-handler provided by task-box
    ;
    ;WARNINGS WHEN USED IN CONCURRENT:
    ;- Will not return if any tasks are blocking via cch-get! or cch-put!
    ;- Do *not* use (set-timer) inside a task, will break implementation
    ;- To make modification of shared memory safe, invoke (set-unsafe!). This 
    ;  forces (concurrent) to execute linearly. Use (set-safe!) to restore 
    ;  normal asynchronous execution.
    get-concurrent-handler

    ;  (go task-box thunk) -> void
    ;  (go task-box thunk fuel) -> void
    ;Enqueues a thunk into a task-box in order to be executed by (concurrent). 
    ;If (go) was invoked by a thunk currently running in (concurrent), the 
    ;newly enqueued thunk will also be asynchronously executed.
    go

    ;  (set-safe! task-box) -> void
    ;Enable concurrent execution in (concurrent). This is #t by default and 
    ;only set #f when (set-unsafe!) is called. 
    set-safe!

    ;  (set-unsafe! task-box) -> void
    ;When this mode is active it forces (concurrent) to execute linearly 
    ;(disables asynchronous execution). This makes modification to 
    ;shared/global values safe. concurrent-channels use these internally.
    ;
    ;A common example of modifying a shared value is writing to 
    ;(current-output-port) with (display) or (printf). Thus to ensure a correct
    ;arbitrary print:
    ;(set-unsafe!)
    ;(printf "foo!\n")
    ;(set-safe!)
    set-unsafe!

    ;  (unsafe? task-box) -> bool
    ;Returns #t if in unsafe mode, otherwise #f
    unsafe?

    ;  (concurrent-sleep task-box nsec) -> void
    ;Use within currently running task to cause that sleep for a minimum of 
    ;nsec without blocking (concurrent) execution.
    concurrent-sleep

    ;  (running? task-box) -> bool
    ;Return whether task-box is currently running. If #f, safe to modify values 
    ;running in (concurrent) with given task-box.
    concurrent-running?

    ;  (pause-concurrent task-box) -> void 
    ;Tell (concurrent) task-box to pause execution. Can check if (concurrent) 
    ;is running with (concurrent-running? task-box). Only safe to execute from
    ;from an outside thread, executing inside a running task will freeze 
    ;execution and will not restart until an outside thread unpauses it.
    pause-concurrent

    ;  (unpause-concurrent task-box) -> void
    ;Tell (concurrent) task-box to resume execution. Can check if (concurrent) 
    ;is running with (concurrent-running? task-box)
    unpause-concurrent

    ;  (make-concurrent-channel task-box) -> concurrent-channel  
    ;make a channel capable of communicating both between asynchronous tasks 
    ;running in (concurrent) with given task-box.
    make-concurrent-channel 

    ;  (concurrent-channel? a) -> boolean
    ;returns #t if argument a is a concurrent-channel, else returns #f
    concurrent-channel?

    ;  (ch-get! concurrent-channel) -> any
    ;Get value from concurrent channel, blocking until available.
    cch-get!

    ;  (ch-try-get! concurrent-channel) -> any
    ;Try to get a value from concurrent channel, returning failure value 
    ;otherwise.
    cch-try-get!

    ;  (ch-put! concurrent-channel any) -> void
    ;Put a value into a concurrent-channel, blocking until space available.
    cch-put!

    ;  (ch-try-put! concurrent-channel any) -> void
    ;Try to put a value into a concurrent-channel, returning failure value 
    ;otherwise. 
    cch-try-put!

    ;  (cch-empty? concurrent-channel) -> boolean
    ;Returns #t if channel is empty, else returns #f
    cch-empty?

    ;  (cch-full? concurrent-channel) -> boolean
    ;Returns #t if channel is full, else returns #f
    cch-full?

    ;test bindings
    concurrent-debug
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


  ;PUBLIC API
  ;  (concurrent concurrent-handler) -> list-of-results
  ;concurrent-handler is a function that takes 1 input argument. (concurrent) 
  ;will execute (concurrent-handler) with the result of the most recent task 
  ;(starting with during the first evaluation). The output of concurrent-handler 
  ;should be a task record. (concurrent) will return results when 
  ;concurrent-handler returns anything that is not a task record.
  (define (concurrent concurrent-handler)
    ;execute tasks in concurrent
    (define (exec-tasks handler results last-result)
      ; pull the next task of the handler 
      (let ([task (handler last-result)])
        (if (task? task)
            ; get task fuel value because it's used more than once
            (let ([fuel (task-fuel task)])
              ; execute task engine and handle results
              (let ([engine-ret ((task-engine task)
                                 fuel
                                 ;#t if completed
                                 (lambda (fuel ret-vals) (list #t ret-vals fuel)) 
                                 ;#f is still running
                                 (lambda (new-engine) (list #f new-engine)))]) 
                (if (car engine-ret) 
                    (exec-tasks handler (append results (list (cadr engine-ret))) cur-result)
                    (exec-tasks handler results cur-result))))
            results)))
    ; execute tasks in concurrent
    (exec-tasks concurrent-handler '() '()))


  ; task timeout 'mutex' functions
  (define (set-unsafe! tb) (task-box-unsafe-timeout-set! tb #t))
  (define (set-safe! tb) (task-box-unsafe-timeout-set! tb #f))
  (define (unsafe? tb) (task-box-unsafe-timeout tb))

  ; get a thread id whether or not this is running in threaded chezscheme
  (define (safe-get-thread-id) (if (threaded?) (get-thread-id) 0))

  (define (concurrent-sleep task-box nsec)
    (set-unsafe!)
    (task-box-channel-waiting-flag-set! task-box #t)
    (task-box-channel-waiting-nsec-set! task-box nsec)
    (set-safe!)
    (engine-block))


  (define-record-type
    task-box 
    (fields (mutable task-queue)
            (mutable waiting-gets)
            (mutable waiting-puts)
            (mutable running)
            (mutable pause)
            (mutable num-running-tasks)
            (mutable waiting-task-flag)
            (mutable waiting-task-nsec)
            (mutable waiting-tasks)
            (mutable channel-get-block-flag) ;indicates current task is blocked on a get!
            (mutable channel-put-block-flag) ;indicates current task is blocked on a put!
            (mutable channel-block-id) ;id task is blocked waiting for
            (mutable channel-non-empty-flag) ;indicates 1 or more channels have new data
            (mutable channel-non-empty-ids) ;list of channels with new data
            (mutable channel-non-full-flag) ;indicates 1 or more channels are no longer full
            (mutable channel-non-full-ids) ;list of channels that are no longer full
            (immutable concurrent-handler)) ;handler function used by (concurrent)
    (protocol 
      (lambda (new) 
        (let ([task-queue  (list)]
               [waiting-gets (list)]
               [waiting-puts (list)]
               [running #f]
               [pause #f]
               [waiting-task-flag #f]
               [waiting-task-nsec 0]
               [waiting-tasks (list)]
               [waiting-task-next-timeout 0]
               [num-running-tasks 0]
               [channel-get-block-flag #f]
               [channel-put-block-flag #f]
               [channel-block-id 0]
               [channel-non-empty-flag #f]
               [channel-non-empty-ids (list)]
               [channel-non-full-flag #f]
               [channel-non-full-ids (list)])
          (new task-queue 
               waiting-gets
               waiting-puts
               running
               pause 
               waiting-task-flag 
               waiting-task-nsec 
               waiting-tasks 
               waiting-task-next-timeout
               num-running-tasks
               channel-get-block-flag
               channel-put-block-flag
               channel-block-id
               channel-non-empty-flag
               channel-non-empty-ids
               channel-non-full-flag
               channel-non-full-ids 
               (lambda (result) ;concurrent handler 
                 (define (exec-handler result)

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
                   (define (dequeue-task! tb) ;null? checking happens in (concurrent)
                     (let ([tasks (task-box-task-queue tb)])
                       (let ([task (car tasks)])
                         (set-box! (task-box-task-queue tb) (cdr tasks))
                         task)))
                   ;enqueue a task that's blocked waiting for input from a 
                   ;concurrent-channel
                   (define (enqueue-waiting-get! id task)
                     (set! waiting-gets (append waiting-gets (list (list id task))))
                     (set! channel-get-block-flag #t))

                   ;handle all now non-empty channel ids
                   (define (enqueue-no-longer-waiting-gets!)
                     (map enqueue-tasks-waiting-for-get-id! channel-non-empty-ids)
                     (set! channel-non-empty-flag #f)
                     (set! channel-non-empty-ids (list)))

                   ;execute any tasks waiting for now non-empty concurrent-channel id, 
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
                       (set! waiting '())
                       parsed))

                   ;enqueue a task that's blocked waiting for concurrent-channel to be 
                   ;non-full
                   (define (enqueue-waiting-put! id task)
                     (set! waiting-puts (append waiting-puts (list (list id task))))
                     (set! channel-put-block-flag #f))

                   ;execute any tasks waiting for now non-empty concurrent-channel id, 
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
                     (map enqueue-tasks-waiting-for-put-id! channel-non-full-ids)
                     (set! channel-non-full-flag #f)
                     (set! channel-non-full-ids (list)))

                   ; checks if outside code is requesting a pause in concurrent execution 
                   ; and wait for unpause.
                   (define (check-for-pause)
                     (define (wait-loop)
                       (when pause tb
                         (let ()
                          (default-sleep)
                          (wait-loop))))
                     (when pause
                       (let ()
                        (set! running #f)
                        (wait-loop)
                        (set! running #t))))


                   ;handle running if this is the first iteration
                   (when (not running)
                     (set! running #t))

                   ;check for pause in concurrent execution and handle it
                   (check-for-pause)

                   ; if channels have new values, enqueue waiting tasks for those
                   ; channel ids
                   (when channel-non-empty-flag
                     (enqueue-no-longer-waiting-gets!))

                   ; if channels are no longer full, enqueue waiting tasks for those
                   ; channel ids
                   (when channel-non-full-flag
                     (enqueue-no-longer-waiting-puts!))

                   (if (eqv? '() task-queue tb)
                       ; if there's no remaining tasks
                       (if (eqv? '() waiting tb)
                           ; if there's no waiting tasks, return results 
                           (let ()
                            (task-box-running-set! tb #f)
                            results)
                           ; otherwise sleep (waiting on input from outside thread) and 
                           ; recurse exec-handler
                           (let ()
                            (default-sleep)
                            (exec-handler tb results)))
                       (if (car result)
                           ; task completed, do cleanup and recurse exec-tasks
                           (let ([num-running (task-box-num-running-tasks tb)])
                             (when (unsafe? tb) (set-safe! tb))
                             (task-box-num-running-tasks-set! tb (- num-running 1))))
                       (let ()
                        (if (unsafe? tb) 
                            ; if execution is set to unsafe mode continue execution
                            ; linearly until safe to execute in concurrent
                            (let ([new-task (make-task (cadr engine-ret) fuel #t)])
                              (dprint tb (list "exec-tasks unsafe new task: " new-task))
                              (enqueue-task-as-head! tb new-task))
                            (let ([new-task (make-task (cadr engine-ret) fuel #t)])
                              (if channel-get-block-flag
                                  ; if task is blocked on a concurrent-channel get!
                                  (enqueue-waiting-get! channel-block-id new-task)
                                  (if channel-put-block-flag
                                      ; else if task is blocked on a concurrent-channel put!
                                      (enqueue-waiting-put! (task-box-channel-block-id tb) new-task)
                                      ; otherwise enqueue task to tail of task list
                                      (enqueue-task! tb new-task)))
                              ; recurse exec-tasks
                              (dequeue-task!)))))))


               (exec-handler result))]))))



; enqueue the new task at the beginning of the task queue
(define (enqueue-task-as-head! tb task)
  (if (task? task)
      (set-box! (task-box-task-queue tb) (append (list task) (task-box-task-queue tb)))
      (let ([o (open-output-string)])
        (fprintf o "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)
        (raise-error (get-output-string o)))))


; indicate to running (concurrent) environment the current task is blocked on a
; get! call
(define (set-get-block! tb id)
  (set-unsafe! tb)
  (task-box-channel-block-id-set! tb id)
  (task-box-channel-get-block-flag-set! #t)
  (set-safe! tb))


; indicate to running (concurrent) environment the current task is blocked on a
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


; pause execution of (concurrent)
(define (pause-concurrent tb)
  (define (wait-for-pause)
    (when (task-box-running tb)
      (let ()
       (default-sleep)
       (wait-for-pause))))
  (task-box-pause-set! tb #t)
  (wait-for-pause)) 


; unpause execution of (concurrent)
(define (unpause-concurrent tb)
  (task-box-pause-set! tb #f)) 


(define (concurrent-running? tb)
  (task-box-running tb))


; create an incrementor function that always returns (+ prev-return-val 1) 
; starting at 0
(define get-incrementor (lambda ()
                          (let ([id 0]) 
                            (lambda () (let ([ret id]) 
                                         (set! id (+ id 1)) 
                                         ret)))))

;function that always emits a unique numerical value per invocation
(define incrementor (get-incrementor))


(define-record-type bq
                    (fields
                      (immutable data)
                      (mutable head)
                      (mutable tail)
                      (mutable size)
                      (immutable full?)
                      (immutable empty?))
                    (protocol
                      (lambda (new)
                        (lambda (bound)
                          (let ([size 0])
                            (new 
                              (make-vector bound) 
                              0 
                              0
                              size
                              (lambda () ;full? 
                                (eqv? size bound))
                              (lambda () ;empty?
                                (eqv? size 0))))))))

(define dequeue!
  (lambda (q)
    (let ([head (bq-head q)])
      (cond
        [(= head (bq-tail q)) #f]
        [else
         (bq-size-set! q (+ (bq-size q) 1))
         (bq-head-set! q (incr q head))
         (vector-ref (bq-data q) head)]))))

(define enqueue!
  (lambda (q item)
    (let* ([tail (bq-tail q)] [tail^ (incr q tail)])
      (cond
        [(= tail^ (bq-head q)) #f]
        [else
         (vector-set! (bq-data q) tail item)
         (bq-tail-set! q tail^)
         (bq-size-set! q (- (bq-size q) 1))])))) 

;record that contains threadsafe data passing functions. Can be created 
;without arguments to create a default record OR with custom communication 
;object and functions get!, put!, empty? and full? and return value failure 
;(for when the object is empty or full) that operate on said communication 
;object.
(define-record-type
  concurrent-channel
  (fields 
    (mutable task-box)
    (mutable obj)
    (immutable id)
    (immutable failure)
    (immutable get!)
    (immutable put!)
    (immutable empty?)
    (immutable full?))
  (protocol
    (lambda (new)
      (lambda (tb size) ;default concurrent-channel case
        (let* ([obj (make-bq size)]
                [id (incrementor)]
                [failure #f]
                [internal-get! (lambda ()
                                 (if (empty? obj)
                                     failure
                                     (let ()
                                       (set-unsafe!)
                                       (append-non-full-channel-id! tb id)
                                       (dequeue! obj)
                                       (set-safe!))))]
                [internal-put! (lambda (val)
                                 (if (full? obj)
                                     failure
                                     (let ()
                                       (set-unsafe!)
                                       (append-non-empty-channel-id! tb id)
                                       (enqueue! obj val)
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
               (lambda () #f)))])))) ;default full? 


  ;for use in cch-get! and cch-put!. Returns whether a given channel is being 
  ;used within a task-box running in concurrent within the same process and 
  ;thread
  (define (in-running-task? tb)
    (if (eqv? (task-box-thread-id tb) (safe-get-thread-id))
        (if (eqv? (task-box-running tb) #t)
            #t
            #f)
        #f))


  (define (error-not-concurrent-channel ch)
    (let ([o (open-output-string)])
      (fprintf o "[cch-get!] provided ch ~a is not a concurrent-channel record\n" ch)
      (raise-error (get-output-string o))
      (values)))


  ;explaining the implementation of the following concurrent-channel manipulation 
  ;functions.
  ;
  ;concurrent-channel-get!/put! are used by tasks running in concurrent and blocks 
  ;by default. This blocking behavior is managed by (concurrent) itself. 
  ;cch-try-get!/put! implement an empty?/full? check inside of unsafe!/safe! 
  ;calls, ensuring the the behavior is atomic.
  ;
  ;Since concurrent-channel-external-get!/put! are used by code *not* running in 
  ;(concurrent) blocking behavior is not available and thus these functions are 
  ;try-get!/try-put! by default. Thus we implement busy waiting to create 
  ;blocking behavior. Data safety is ensured by pause-concurrent/unpause-concurrent
  ;calls, which ensure that (concurrent) is paused before we attempt to modify 
  ;data.


  ;PUBLIC API
  ;  (cch-get! ch) -> any
  ;blocking get
  (define (cch-get! ch)
    (if (concurrent-channel? ch)
        (let ([tb (concurrent-channel-task-box ch)])
          ;cch-get! from running (concurrent) tasks block by default
          ((concurrent-channel-get! ch)))
        (error-not-concurrent-channel)))

  ;PUBLIC API
  ;  (cch-try-get! ch) -> any
  ;non-blocking get
  (define (cch-try-get! ch)
    (if (concurrent-channel? ch)
        (let ([tb (concurrent-channel-task-box ch)])
          ;must implement try-get behavior for cch-try-get! calls from within 
          ;tasks running in (concurrent)
          (let ()
           (set-unsafe!)
           (if (cch-empty? ch)
               (concurrent-channel-failure ch)
               ((concurrent-channel-get! ch)))
           (set-safe!)))
        (error-not-concurrent-channel)))

  ;PUBLIC API
  ;  (cch-put! ch val) -> void
  (define (cch-put! ch val)
    (if (concurrent-channel? ch)
        (let ([tb (concurrent-channel-task-box ch)])
          ((concurrent-channel-put! ch) val))
        (error-not-concurrent-channel)))

  ;PUBLIC API
  ;  (cch-try-put! ch val) -> void
  (define (cch-try-put! ch val)
    (if (concurrent-channel? ch)
        (let ([tb (concurrent-channel-task-box ch)])
          ((concurrent-channel-put! ch) val))
        (error-not-concurrent-channel)))

  ;PUBLIC API
  ;  (cch-empty? ch) -> boolean
  (define (cch-empty? ch)
    (if (concurrent-channel? ch)
        ((concurrent-channel-empty? ch))
        (error-not-concurrent-channel)))

  ;PUBLIC API
  ;  (cch-empty? ch) -> boolean
  (define (ch-full? ch)
    (if (concurrent-channel? ch)
        ((concurrent-channel-full? ch))
        (error-not-concurrent-channel)))


  (define concurrent-debug #f)
  (define dprint
    (lambda (tb args)
      (when concurrent-debug
        (let ()
         (set-unsafe! tb)
         (display "[concurrent-DEBUG-PRINT] ")
         (map display args)
         (display "\n")
         (flush-output-port)
         (set-safe! tb)))))


  ;PUBLIC API
  ;  (get-concurrent-handler task-box) -> concurrent-handler
  ;A concurrent handler is a function that takes 1 value and returns a task 
  ;for (concurrent) to execute
  (define (get-concurrent-handler tb)
    (task-box-concurrent-handler tb))

  ;PUBLIC API
  ;  (go thunk) -> void
  ;  (go thunk fuel) -> void
  ;enqueue a thunk to be executed in a background taskbox
  (define go
    (let ([go-enqueue! 
           (lambda (tb task)
             (if (task? task)
                 (let ([num-running (task-box-num-running-tasks tb)])
                   (task-box-num-running-tasks-set! tb (+ num-running 1))
                   (set-box! (task-box-task-queue tb) (append (task-box-task-queue tb) (list task))))
                 (let ([o (open-output-string)])
                   (fprintf o "[enqueue-task!] provided task ~a is not a task record, task thrown out\n" task)
                   (raise-error (get-output-string o)))))])
      (case-lambda
        [(tb thunk) (go-enqueue! tb (make-task thunk))]
        [(tb thunk fuel) (go-enqueue! tb (make-task thunk fuel))]))))
