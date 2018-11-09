;comments begin with a semicolon

#lang racket 

;this is a library import statement for the 3 library
(require "3.rkt")

;this is a statement that acts like a C++ header if someone imports pres.rkt,
;it makes the following functions available
(provide coroutine-example
         go-example
         message-example
         yield-example
         non-trivial-computation-example)


;------------------------------------------------------------------------------
;defining a function named 'coroutine-example'
(define (coroutine-example)

  ;define coroutine generator
  (define-coroutine 
    (example-coroutine-generator x)
    (printf "you gave me ~a" x))

  ;generate suspended coroutine with input argument 5
  (define suspended-coroutine (example-coroutine-generator 5))

  ;run coroutine
  (suspended-coroutine) 

  ;return nothing
  (void)) 




;------------------------------------------------------------------------------
(define (go-example)

  ;create a new scope with a new datapool connected to a new datapool with 2 
  ;worker threads
  (let ([datapool (make-datapool (make-computepool 2))])

    (define-coroutine
      (example-go-coroutine x)
      (printf "you gave me ~a\n" x))

    ;send a suspended coroutine as a new task for the worker threads
    (go datapool (example-go-coroutine 27))

    ;kill worker threads
    (close-dp datapool))) 




;------------------------------------------------------------------------------
(define (message-example)

  ;create a new scope with several new variables
  (let* ([datapool (make-datapool (make-computepool 2))]
         ;message handlers activate when an incoming message has a matching type
         [example-message-type 'example-type] 
         ;creating a new message using our example type and the payload value '6'
         [example-message (make-message example-message-type 6)])

    (define-coroutine
      (example-handler-coroutine msg)
      (printf "this time you gave me ~a\n" (message-content msg)))

    ;register a new message handler
    (register-message-handler
      datapool
      example-handler-coroutine ;use the generator instead of a suspended coroutine
      example-message-type) ;handler is called for incoming messages of this type)

    ;send our message to be handled
    (send-message datapool example-message)

    (close-dp datapool)))




;------------------------------------------------------------------------------
(define (yield-example)
  ;make a computepool with only 1 worker thread
  (let ([dp (make-datapool (make-computepool 1))]
        ;make an asynchronous channel
        [ch (channel)])

    (define-coroutine
      (example-yield ch)
      (printf "pre-yield\n")
      ;yield value is arbitrary, it is ignored by the worker threads. Generally 
      ;only useful for user managed code where you expect yield results
      (yield 0) 
      ;this line blocks until there's something in the channel to get
      (printf "~a\n" (ch-get ch)))

    ;the previous coroutine cannot complete until this coroutine completes
    (define-coroutine
      (another-coroutine ch)
      (ch-put ch "hello-world!\n"))

    (go dp (example-yield ch))
    (go dp (another-coroutine ch))

    (close-dp dp)))



;------------------------------------------------------------------------------
;For this example, we'll look at doing non-trivial computation. Say we run a 
;web service like https://www.dcode.fr/prime-numbers-search, where you want to
;return to the user a requested nth prime number. Here's how you could 
;calculate it using 3
(define (non-trivial-computation-example nth)

  ;defining a coroutine to calculate the cpu intensive task of calculating the 
  ;nth prime, for use in later examples
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
