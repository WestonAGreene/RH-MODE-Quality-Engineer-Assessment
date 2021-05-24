# RH-MODE-Quality-Engineer-Assessment
Assessment for a Quality Engineer on the Red Hat Marketing Operation Date Engineering team

Please complete the following:
1) List the edge cases that you believe should be accounted for in the following scenario:
    Out team is replacing functionality from our Marketing Automation Platform, Eloqua, with real-time stream-processing based on Apache Kafka and Python. Our current project is to extract consent processing. Consent messages are received via form submissions. The submissions contain, among other fields, `consent_time` and `consent`. A `consent` of "1" means the customer does want to receive marketing emails and a "0" means they do not. Using email address as their unique identifier, we keep track of their current status.

1) Refactor the following code to be self-documenting 
    We believe that tests should be treated like code. Althought a refactor is not a normal QA role, it is telling of how serious you take your tests.  
    We follow principles from Clean Code by Robert Cecil Martin.
    This is an atrocious script from 4 years ago when I was learning to code. I redacted a few lines but much of it is verbatim what is running in production today. Because running it would require our credentials, I do not expect the refactor to run. I am looking for concepts and creativity.

1) Verbalize what is done by the code you just refactored in a doc string at the top.

1) Create unit tests for that same above code

1) Return the assessment as a Pull Request on this repo.