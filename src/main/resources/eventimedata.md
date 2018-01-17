## Event Time Example
//The first records is for time Wed, 27 Apr 2016 11:34:22 GMT.

1461756862000,"aapl",500.0

// Event after 5 seconds

1461756867001,"aapl",600.0

// Event after 11 seconds

1461756872000,"aapl",400.0


## Late Events Example

//It’s an event is for Wed, 27 Apr 2016 11:34:27 which is 5 seconds before the last event.

1461756867001,"aapl",200.0


## Session Window Input

// start two sessions

session1,100
session2,200

// Additional Event for Session 1

session1,200

// End Session 1

session1,200,end


// Starting new session1 and updating existing session 2

session1,100
session2,200









