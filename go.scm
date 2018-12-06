(library 
  (go)
  (export
    ;;;COMMUNICATION
    channel
    make-channel ;create an asynchronous channel for inter-thread communication
    ch-get! ;get from a channel
    ch-put! ;send into a channel
    channel-empty? ;#t if number of items is 0, else #f

    ;;;COMPUTATION 
    make-computepool ;start a computation pool of worker threads
    close-computepool ;inform the computepool to end execution
    wait-computepool ;wait until there are no executing or waiting tasks
    set-default-num-threads ;set default number of computepool threads
    thunk ;create a thunk from input (ie (lambda () (your-expressions-here)))
    go) ;place a thunk in a channel to be executed by a thread 
  (import (rnrs)
          (chezscheme))


  ;;;----------------------------------------------------------------------------
  ;;; COMMUNICATION
  ;;;----------------------------------------------------------------------------
  (define-record-type 
    channel
    (fields
      (immutable data)
      (mutable head)
      (mutable tail)
      (mutable len)
      (immutable mutex)
      (immutable ready)
      (immutable room))
    (protocol
      (lambda (new)
        (lambda (bound)
          (new (make-vector bound) 0 0 0 (make-mutex)
               (make-condition) (make-condition)))))) 

  (define (channel-length ch)
    (channel-len ch))

  (define (channel-empty? ch)
    (echv? (channel-len ch) 0))

  (define ch-get!
    (lambda (ch [block #t])
      (with-mutex (channel-mutex ch)
                  (let loop ()
                    (let ([head (channel-head ch)])
                      (cond
                        [(= head (channel-tail ch))
                         (if block
                           (let ()
                             (condition-wait (channel-ready ch) (channel-mutex ch))
                             (loop))
                           #f)]
                        [else
                          (channel-head-set! ch (incr ch head))
                          (channel-len-set! ch (+ 1 (channel-len ch)))
                          (condition-signal (channel-room ch))
                          (vector-ref (channel-data ch) head)])))))) 

  (define ch-put!
    (lambda (item ch)
      (with-mutex (channel-mutex ch)
                  (let loop ()
                    (let* ([tail (channel-tail ch)] [tail^ (incr ch tail)])
                      (cond
                        [(= tail^ (channel-head ch))
                         (condition-wait (channel-room ch) (channel-mutex ch))
                         (loop)]
                        [else
                          (vector-set! (channel-data ch) tail item)
                          (channel-tail-set! ch tail^)
                          (channel-len-set! ch (- 1 (channel-len ch)))
                          (condition-signal (channel-ready ch))])))))) 

  (define incr
    (lambda (ch i)
      (modulo (+ i 1) (vector-length (channel-data ch)))))


  ;;;----------------------------------------------------------------------------
  ;;; COMPUTEPOOL
  ;;;---------------------------------------------------------------------------- 
  ;;create computepool cpironment of worker threads
  (define (make-computepool [num-threads *DEFAULT-NUM-THREADS*])
    (let ([ret (vector 
                 (vector ;datapool info vector 
                   num-threads
                   [running-flag #t])
                 (make-vector ;threads, channels, and mutexs
                   num-threads))])
      (for ([i num-threads])
           (let ()
             (vector-set! 
               (vector-ref ret 1) 
               i
               (vector 
                 (fork-thread (thunk (cp-thread-start ret))) ;thread
                 (make-channel *MAX-CP-CHANNEL-SIZE*) ;thread task channel
                 (make-mutex) ;thread task channel mutex
                 (make-condition) ;resume condition
                 (make-mutex))))) ;resume mutex
      ret))


  ;; Global for default computepool pointer
  (define *DEFAULT-COMPUTEPOOL* 'unset)

  (define *DEFAULT-NUM-THREADS* 1)

  (define (set-default-num-threads num)
    (set! *DEFAULT-NUM-THREADS* num))

  ;; Global for maximum datapool task channel size
  (define *MAX-CP-channel-SIZE* 255)

  ;; Global for engine fuel
  (define *GO-ENGINE-FUEL* 50)


  (define (close-computepool cp)
    (vector-set! (vector-ref cp 0) 1 #f))


  (define (get-running-flag cp)
    (vector-ref (vector-ref cp 0) 1))


  ;; PUBLIC API
  ;; Return number of threads in the datapool
  (define (get-num-cp-threads cp) 
    (vector-ref (vector-ref cp 0) 0))


  ;; Return channel of sleeping threads waiting to invoke (go)
  (define (get-waiting-threads-channel cp) 
    (vector-ref (vector-ref cp 0) 1))


  ;; Return channel of sleeping threads waiting to invoke (go)
  (define (get-waiting-threads-channel-mut cp) 
    (vector-ref (vector-ref cp 0) 2))


  ;;;----------------------------------------------------------------------------
  ;;; COMPUTATION - thread management
  ;;;----------------------------------------------------------------------------
  ;;;threads
  ;; Get a thread pid at provided index
  (define (get-cp-thread cp idx)
    (vector-ref 
      (vector-ref 
        (vector-ref 
          cp
          1) 
        idx) 
      0))


  ;; Get a thread task channel at provided index
  (define (get-cp-channel cp idx)
    (vector-ref 
      (vector-ref 
        (vector-ref 
          cp
          1) 
        idx) 
      1))


  ;; Get a thread task channel mutex at provided index
  (define (get-cp-channel-mut cp idx)
    (vector-ref 
      (vector-ref 
        (vector-ref 
          cp
          1) 
        idx) 
      2))


  ;; Get a thread task channel mutex at provided index
  (define (get-cp-resume-cond cp idx)
    (vector-ref 
      (vector-ref 
        (vector-ref 
          cp
          1) 
        idx) 
      3))


  ;; Get a thread task channel mutex at provided index
  (define (get-cp-resume-mut cp idx)
    (vector-ref 
      (vector-ref 
        (vector-ref 
          cp
          1) 
        idx) 
      4))


  ;; Get the index of the fullest thread task channel
  (define (get-max-cp-q-idx cp)
    (define longest (cons (channel-length (get-cp-channel cp 0)) 0))
    (when (> (get-num-cp-threads cp) 1)
      (for ([i (in-range 1 (get-num-cp-threads cp))])
           (let ([cur-q-len (channel-length (get-cp-channel cp i))])
             (when (< (car longest) cur-q-len)
               (set! longest (cons cur-q-len i))))))
    (cdr longest))


  ;; Get the index of the emptiest thread task channel
  (define (get-min-cp-q-idx cp)
    (define shortest (cons (channel-length (get-cp-channel cp 0)) 0))
    (when (> (get-num-cp-threads cp) 1)
      (for ([i (in-range 1 (get-num-cp-threads cp))])
           (let ([cur-q-len (channel-length (get-cp-channel cp i))])
             (when (> (car shortest) cur-q-len)
               (set! shortest (cons cur-q-len i))))))
    (cdr shortest))


  ;; PUBLIC API
  (define (go thunk [out-channel #f] [fuel *GO-ENGINE-FUEL*] [cp *DEFAULT-COMPUTEPOOL*])
    (when (eqv? cp 'unset) 
      (let ()
        (set! *DEFAULT-COMPUTEPOOL* (make-computepool *DEFAULT-NUM-THREADS*))
        (set! cp *DEFAULT-COMPUTEPOOL*)))
    (go-engine cp (make-engine thunk) out-channel fuel))


  ;; Create a (lambda () (...)) thunk from input
  (define-syntax thunk
    (syntax-rules () [(thunk ...) (lambda () (...))]))


  ;; ch-put a task to be executed
  (define (go-engine cp engine output-channel fuel)
    (let ([q-idx (get-min-cp-q-idx cp)])
      (mutex-acquire (get-cp-channel-mut cp q-idx))
      (ch-put! (list engine out-channel fuel) (get-cp-channel cp q-idx))
      (mutex-release (get-cp-channel-mut cp q-idx)))

    ;resume cp-thread if waiting on condition
    (condition-signal (get-cp-resume-cond cp (get-cp-thread cp q-idx)))
    #t)


  ;;;----------------------------------------------------------------------------
  ;;; COMPUTATION - thread functions
  ;;;----------------------------------------------------------------------------

  ;; Return thread's channel index if not empty, otherwise gets the index of the 
  ;; fullest channel.
  (define (get-task-q-idx cp thread-idx)
    (let ([thread-channel (get-cp-channel cp thread-idx)])
      (if (equal? (channel-length thread-channel) 0)
        (let ([highest-idx (get-max-cp-q-idx cp)])
          (if (equal? (channel-length (get-cp-channel cp highest-idx)) 0) 
            #f
            highest-idx))
        thread-idx)))


  ;; Return a task from a thread channel to execute
  (define (get-task cp thread-idx)
    (let ([q-idx (get-task-q-idx cp thread-idx)])
      (if (equal? q-idx #f)
        #f 
        (let ()
          (mutex-acquire (get-cp-channel-mut cp q-idx))
          (let ([ret 
                  (if (channel-empty? (get-cp-channel cp q-idx))
                    #f
                    (ch-get! (get-cp-channel cp q-idx)))])
            (mutex-release (get-cp-channel-mut cp q-idx))
            ret)))))


  ;; Evaluate given task, limits cpu starvation by limiting continuous 
  ;; evaluations in coroutines. If provided task is *not* a coroutine and/or 
  ;; that coroutine does not (yield) intelligently this may have no effect.
  ;; Returns: #t if task completed, #f if task not yet completed
  (define (cp-thread-exec-task cp thread-idx task)
    (let ([engine (car task)]
          [return-dest (cadr task)]
          [fuel (caddr task)])
      (engine 
        fuel
        (lambda (fuel return-val) ;task completed 
          (let()
            (when (and return-dest return-vals)
              (ch-put! return-val return-dest))
            #t))
        (lambda (new-engine)
          (let ()
            (go-engine cp new-engine return-dest) ;place task at the back of a channel
            #f)))))


  ;; Eternal thread tail recursion of executing tasks
  (define (cp-thread cp thread-idx) 
    (with-exception-handler
      (lambda (e)
        (map display
             '("cp-thread got error, current task thrown out: ~a\n" e)
             (current-error-port))
        (cp-thread cp thread-idx))
      (lambda ()
        (let ([task (get-task cp thread-idx)])
          (if (or (not task) (equal? (car task) #f))
            (let ([mut (get-cp-resume-mut cp (get-thread-id))]
                  [condition (get-cp-resume-cond cp (get-thread-id))])
              (mutex-acquire mut)
              (condition-wait condition mut)
              (mutex-release mut))
            (let ()
              (cp-thread-exec-task ;execute the task we get
                cp 
                thread-idx 
                task)))
          (when (get-running-flag cp)
            (cp-thread cp thread-idx))))))


    ;; Thread startup coroutine
    (define (cp-thread-start cp)
      (let ([id (current-thread)])
        (define thread-num 0)
        (for ([i (get-num-cp-threads-intern cp)])
             (when (equal? (get-cp-thread cp i) id) 
               (set! thread-num i)))
        (cp-thread cp thread-num)))


    ;; Print out the lengths of all datapool cpironment worker task channels
    (define (print-channel-lens cp)
      (newline)
      (do ([i 0 (+ i 1)])
        ((eqv? i (get-num-cp-threads cp)) '())
        (map display '("len task q " 
                       i 
                       ": " 
                       (channel-length (get-cp-channel (get-cp cp))) 
                       "\n")))
      (newline))


    (define (print-thread-states cp)
      (let ([num (get-num-cp-threads cp)])
        (do ([i 0 (+ i 1)]) 
          ((eqv? i num) '())
          (let ([thread (get-cp-thread cp i)])
            (if (thread-running? thread)
              (map display '("thd #" i ": Running\n"))
              (if (thread-dead? thread)
                (map display '("thd #" i ": Dead\n"))
                (map display '("thd #" i ": Sleeping\n")))))))
      (newline))


    ;; Wait until total task channel lengths == 0 and threads are asleep and return 
    ;; time in (decimal) milliseconds waited. If print is set to #t, will 
    ;; periodically print channel lengths
    (define (wait-computepool cp [print #f])
      (define (wait-loop cp 
                         [start-time (current-inexact-milliseconds)]
                         [check-in-time (current-inexact-milliseconds)] 
                         [idxs (build-list (get-num-cp-threads cp)
                                           (lambda (i) i))])
        (let ([running (ormap (lambda (idx)
                                (if (> (channel-length (get-cp-channel cp idx)) 0)
                                  #t
                                  (if (thread-running? (get-cp-thread cp idx))
                                    #t
                                    #f)))
                              idxs)])
          (if running
            (if (and print 
                     (> (- (current-inexact-milliseconds) check-in-time) 5000))
              (let ([new-check-in (current-inexact-milliseconds)])
                (print-channel-lens cp)
                (print-thread-states cp)
                (display "\nWaiting for all tasks to complete...\n")
                (sleep 0.01)
                (wait-loop cp start-time new-check-in idxs))
              (let ()
                (sleep 0.01)
                (wait-loop cp start-time check-in-time idxs)))
            (- (current-inexact-milliseconds) start-time))))
      (let ([wait-time (wait-loop cp)])
        (when print 
          (let ()
            (print-channel-lens cp)
            (map display '("Waited for ~a milli\n" wait-time))))
        wait-time)))
