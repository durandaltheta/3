;comments begin with a semicolon  

;The following is a library of example code that shows some of what 3 can do

;Specify the dialect of racket we're running
#lang racket 

;this is a library import statement for the 3 library
(require "3.rkt")

;this is a statement that acts like a C++ header if someone imports pres.rkt,
;it makes the following functions available
(provide coroutine-ex
         go-ex 
         message-ex
         yield-ex 
         testing-ex
         interaction-ex
         datapool-ex
         redirect-go-ex
         redirect-handler-ex
         non-trivial-computation-ex)


;------------------------------------------------------------------------------
;defining a function named 'coroutine-ex'
(define (coroutine-ex)

  ;define coroutine generator
  (define-coroutine 
    (ex-coroutine-generator x)
    (printf "you gave me ~a" x))

  ;generate suspended coroutine with input argument 5
  (define suspended-coroutine (ex-coroutine-generator 5))

  ;run coroutine
  (suspended-coroutine) 

  ;return nothing
  (void)) 




;------------------------------------------------------------------------------
(define (go-ex)

  ;create a new scope with a new datapool connected to a new 
  ;datapool with 2 worker threads
  (let ([datapool (make-datapool (make-computepool 2))])

    (define-coroutine
      (ex-go-coroutine x)
      (printf "you gave me ~a\n" x))

    ;send a suspended coroutine as a new task for the worker threads
    (go datapool (ex-go-coroutine 27))

    ;wait for tasks to be completed
    (wait-dp datapool)

    ;kill worker threads
    (close-dp datapool))) 




;------------------------------------------------------------------------------
(define (message-ex)

  ;create a new scope with several new variables
  (let* ([datapool (make-datapool (make-computepool 2))]
         ;message handlers activate when an incoming message has a matching type
         [ex-message-type 'ex-type] 
         ;creating a new message using our ex type and the payload value '6'
         [ex-message (make-message ex-message-type 6)])

    (define-coroutine
      (ex-handler-coroutine msg)
      (printf "this time you gave me ~a\n" (message-content msg)))

    ;register a new message handler
    (register-message-handler
      datapool
      ex-handler-coroutine ;use the generator instead of a suspended coroutine
      ex-message-type) ;handler is called for incoming messages of this type)

    ;send our message to be handled
    (send-message datapool ex-message)
    (wait-dp datapool)
    (close-dp datapool)))




;------------------------------------------------------------------------------
(define (yield-ex)
  ;make a computepool with only 1 worker thread
  (let ([dp (make-datapool (make-computepool 1))]
        ;make an asynchronous channel
        [ch (channel)])

    (define-coroutine
      (ex-yield ch)
      (printf "pre-yield\n")
      ;yield value is arbitrary, it is ignored by the worker threads. Generally 
      ;only useful for user managed code where you expect yield results
      (yield 0) 
      ;this line blocks until there's something in the channel to get
      (printf "~a" (ch-get ch)))

    ;the previous coroutine cannot complete until this coroutine completes
    (define-coroutine
      (another-coroutine ch)
      (ch-put ch "hello-world!\n"))

    (go dp (ex-yield ch))
    (go dp (another-coroutine ch))
    (wait-dp dp)
    (close-dp dp)))




;------------------------------------------------------------------------------
;By writing coroutines that do not access external state we can more easily 
;test the api without extra instrumentation. Here are some simple test 
;functions that 3 provides (they are similar to the google c++ test suite)
(define (testing-ex)
  (define-coroutine
    (return-inp inp)
    inp)

  ;reset global test values
  (reset-test-results)

  ;start a new test section, automatically call (print-test-report) if this 
  ;is not the first test-section invoked
  (test-section "example testing")

  (test-true? "Is the output value true?" ((return-inp #t)))
  (test-true? "What about now?" ((return-inp #f)))
  (test-equal? "Can we compare successfully?" ((return-inp 3)) 3)
  (test-equal? "What about now?" ((return-inp 3)) 4)

  ;print the aggregate results of the test
  (print-test-report))




;------------------------------------------------------------------------------
;There's nothing stopping you from accessing external state if that's what is
;required for a given task. Here we pass information between 2 executing (go)
;tasks
(define (interaction-ex)
  ;define coroutine which passes a 'ball' back and forth through channels
  (define-coroutine
    (pass-ball who in out pass-limit)
    (define (intern-recursion who in out pass-limit)
      ;block till we get the ball
      (let ([ball (ch-get in)])
        (printf "\n~a catches the ball\n" who)
        (printf "~a throws the ball\n" who)
        
        ;determine if the game is still going
        (if (equal? ball 'ball)
            (let ([new-limit (- pass-limit 1)])
              (if (equal? new-limit 0)
                  (ch-put out 'done) ;decide we're done playing
                  (let ()
                    (ch-put out ball)
                    (intern-recursion who in out new-limit))))
            (printf "~a takes the ball inside\n" who))))

    (intern-recursion who in out pass-limit))

  (let ([dp (make-datapool (make-computepool 2))]
        [ch1 (channel)]
        [ch2 (channel)]
        [pass-limit 3])
    (go dp (pass-ball "Son" ch1 ch2 pass-limit))
    (go dp (pass-ball "Dad" ch2 ch1 pass-limit))
    (ch-put ch1 'ball)

    (wait-dp dp)
    (close-dp dp)))




;------------------------------------------------------------------------------ 
;In this example we will examine how we can register and retrieve data from a
;datapool
(define (datapool-ex)
  (define data1 3)
  (define data2 "some text")

  (define test-class%
    (class object% 
           (super-new)
           (init-field field1
                       field2)))

  (define data3 (make-object test-class% #f 'a-symbol))


  (let* ([dp (make-datapool (make-computepool 1))]
         [key1 (register-data! dp data1)]
         [key2 (register-data! dp data2)]
         [key3 (register-data! dp data3)])

    (printf "Value of retrieved data1: ~a\n" (get-data dp key1))
    (printf "Value of retrieved data2: ~a\n" (get-data dp key2))
    (printf "Value of retrieved data-object field1: ~a\n" (get-data-field dp key3 'field1))
    (printf "Value of retrieved data-object field2: ~a\n" (get-data-field dp key3 'field2))
    (set-data! dp key1 4)
    (printf "Value of modified data1: ~a\n" (get-data dp key1))
    (close-dp dp)))




;------------------------------------------------------------------------------ 
;In this example we will examine how go invocations can redirect coroutine 
;output to a datapool or other places
(define (redirect-go-ex)

  ;setup our data 
  (define test-data 3)

  ;define a coroutine that outputs data (in this case 5)
  (define-coroutine
    (output-new-test-data)
    5)


  ;create datapools and register our various data
  (let* ([dp (make-datapool (make-computepool 1))]
         ;register a variable to a datapool
         [test-data-key (register-data! dp test-data)])
    (printf "value of test-data: ~a\n" (get-data dp test-data-key))

    (printf "\nRun coroutine where redirected results will modify said data\n\n")
    ;run coroutine with (go) and redirect output to various places
    (go dp (output-new-test-data) (list (list '#:data test-data-key #f)))

    (wait-dp dp)
    (printf "value of test-data: ~a\n" (get-data dp test-data-key))

    (close-dp dp)))



;------------------------------------------------------------------------------
;In this example we will examine how message handlers can be instrumented to 
;gather input values from registered data before executing and how to redirect 
;the handler's output
(define (redirect-handler-ex)
  (let ([dp (make-datapool (make-computepool 2))])

    (define test-data 4)

    (printf "test data initial value: ~a\n\n" test-data)

    (let* ([data-key (register-data! dp test-data)]
           [msg-type 'some-type]
           ;make a new message with specified type, content payload, and source 
           ;key
           [msg (make-message msg-type "arbitrary content!")])

      ;a coroutine that prints message content and a modified input value
      (define-coroutine
        (output-modified msg input)
        (printf "message content: ~a\n" (message-content msg))
        (let* ([a (car input)]
               [new-a (+ a 2)])
          new-a))

      ;register a new message handler
      (register-message-handler 
        dp 
        output-modified 
        msg-type
        #f
        ;specify input values from handler's datapool to get when calling 
        ;output-modified
        (list (list data-key #f))
        ;specify return destinations 
        (list (list '#:data data-key #f)))

      ;send a message to our handler
      (send-message dp msg)

      ;wait for the handler to finish processing
      (wait-dp dp)
      ;print the modified field results
      (printf "\ntest data's new value: ~a\n" (get-data dp data-key))
      (close-dp dp))))




;------------------------------------------------------------------------------
;For this ex, we'll look at doing non-trivial computation. Say we run a 
;web service like https://www.dcode.fr/prime-numbers-search, where you want to
;return to the user a requested nth prime number. Here's how you could 
;calculate it using 3
(define (non-trivial-computation-ex nth)

  ;defining a coroutine to calculate the cpu intensive task of calculating the 
  ;nth prime
  (define-coroutine 
    (find-nth-prime-co n output-channel)

    ;;Return #t if given number is prime, else #f
    (define (is-prime? n [i 2])
      (if (>= i n)
          #t
          (if (equal? 0 (remainder n i))
              #f
              (let ([new-i (+ i 1)])
                (is-prime? n new-i)))))

    ;;Find the nth prime number
    (define (find-nth-prime n [candidate 2] [count 2])
      (if (equal? n 0)
          candidate
          (let ([new-count (+ count 1)]
                [next-n (- n 1)])
            (if (is-prime? count)
                (find-nth-prime next-n count new-count)
                (find-nth-prime n candidate new-count)))))

    (let ([nth-prime (find-nth-prime n)])
      (ch-put output-channel nth-prime)))


  ;launch our cpu intensive task and do other work
  (let* ([output-channel (channel)]
         [my-datapool (make-datapool (make-computepool 1))]
         [suspended-coroutine (find-nth-prime-co nth output-channel)])
    (printf "\nRun our coroutine to find ~ath prime number\n" nth)
    (go my-datapool suspended-coroutine)
    (printf "Now we can do other things or wait for it to finish\n")
    (define (print-loop ch)
      (let ([res (ch-get ch #f)])
        (if res
            (printf "Here's our ~ath prime: ~a\n" nth res)
            (let ()
              (printf "Doing other things!\n")
              (sleep 1.0)
              (print-loop ch)))))
    (print-loop output-channel)
    (close-dp my-datapool))) 
