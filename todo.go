package main

/*

```
In fact, it’s a proven impossibility to absolutely guarantee forward progress in any distributed consensus system that is susceptible to message losses.
```
```
Note that the proposer can always abandon a proposal and forget all about it, as long as it never tries to issue another proposal with the same number.
```



One optimization that can be made in this case, assuming a single stable leader, is to skip the prepare phase.
If we assume that the leadership will remain "sticky", there is no need to continue sending out proposal numbers -
the first proposal number sent out will never be "overridden" since there is only one leader.

^^^^^^^^^ OUR LEADERSHIP IS NOT STICKY ^^^^^^^^^^^^^^^


if i get promises from a majority of nodes and then my accept request is declined i should not be worried,
my value v (if any) was previously ACCEPTED by a majority, so whoever went above my n, has certainly received my value and is now working
towards learning it. TODO:**THINK ABOUT THIS** then why am i retrying the same proposal with just an incremented seq?
Because it makes no sense to assume that the others will do my job, if i managed to speak to a majority of nodes
im in a very good position, therefore i should try to end the transaction.


TODO - LAST UPDATE 05/06/2019

[TESTING FUNCTION]
- think of complex scenarios, related to testing
and show in the python the automatic tests and in the web versions the normal tests


[DOCUMENTATION AND COMMENTS]
- adapt code documentation to godoc standard 		[x] Done
- better api documentation				[ ] ----


[FUTURE]
- update python version (must)						[ ] ---- se ciao

*/
