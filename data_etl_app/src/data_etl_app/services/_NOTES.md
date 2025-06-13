## Ontology Service:
If _init_data is a pure function and its input (the S3 file contents) is guaranteed to be the same for both thread calls, then both threads will compute the same result and assign the same values to self.graph and self._cache. In this specific case, data corruption (in the sense of inconsistent or invalid data) will not occur—the end state will be the same as if only one thread had run _init_data.

However, race conditions can still occur:

Both threads could read and write to self.graph and self._cache at the same time, which is generally unsafe in Python and can lead to unpredictable behavior, especially if the assignment is not atomic or if other code reads these attributes while they are being updated.
If, in the future, _init_data is changed to be impure (e.g., it depends on the current time, a different S3 file, or some other external state), then running it concurrently could result in inconsistent or unexpected state.
Best practice:
Even if the function is pure and the input is the same, using a lock is still recommended to prevent subtle bugs and to future-proof your code. The lock ensures that only one thread can update the shared state at a time, making your code robust to future changes and safe for concurrent use.

Summary:

With a pure function and identical input, you won't get data corruption, but you still risk race conditions and unpredictable behavior.
The lock is a safety measure for both now and future code changes.


## Difference between service and util:
1. util is usually a pure function and stateless, can be used widely by external modules. A service will carry a state and logic to interact with that state, the state may or may not be permanent.
2. Even if a service is: Pure, Procedural, Stateless, it’s still a service if it represents a business capability or use case. In Contrast, a Util: Doesn’t know or care about business context, Isn’t tied to any one domain, Can be reused broadly across unrelated parts of the app.