;comments begin with a semicolon

#lang racket 

;this is a library import statement for the 3 library
(require "3.rkt")

;this is a statement that acts like a C++ header if someone imports pres.rkt,
;it makes the following functions available
(provide coroutine-example
         go-example
         message-example
         yield-example)


;defining a function named 'coroutine-example'
(define (coroutine-example)

  ;define coroutine generator
  (define-coroutine 
    (example-coroutine-generator x)
    (printf "you gave me ~a" x))

  ;generate suspended coroutine with input argument 5
  (define suspended-coroutine (example-coroutine-generator 5))

  (suspended-coroutine) ;run coroutine

  (void)) ;return nothing




(define (go-example)

  ;create a new scope with a new datapool connected to a new datapool with 2 
  ;worker threads
  (let ([datapool (make-datapool (make-computepool 2))])

    (define-coroutine
      (example-go-coroutine x)
      (printf "you gave me ~a\n" x))

    ;send a suspended coroutine as a new task for the worker threads
    (go datapool (example-go-coroutine 27))

    (close-dp datapool)) ;kill worker threads
  (void))




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

    (close-dp datapool))
  (void))




(define (yield-example)
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

    (define-coroutine
      (another-coroutine ch)
      (ch-put ch "hello-world!\n"))

    (go dp (example-yield ch))
    (go dp (another-coroutine ch))

    (wait-len dp #f) ;wait till task queues are empty 
    (close-dp dp)
    (void)))
