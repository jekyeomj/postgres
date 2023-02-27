Strengths of the file format
  - Because customizable data parsing is possible using FDW, it is open to data storage methods and media.
  - Regardless of the system (cloud, distributed file system), it is possible to easily connect to the database.

Weaknesses of the file format
  - Because it uses hooked functions in FDW, it is difficult to share data between functions. That is, since global variables cannot be used, the value must be parsed and used for each function.

Suggestions for improving the file format
  - It would be nice if global variables for FDW were also provided. Commonly accessible pointers, etc.

Feedback on this project
  - This is a common occurrence in any coursework, it would be helpful if you announced the contents of last year's piazza to solve the problem.
  - This project initially took some time to understand the concept of FDW and understand at when each function was called, but after that it was not hard.
  - The results are different depending on the gradescope speed. In this part, I hope that the problem of constantly changing results, such as using the average or minimum of the results of multiple runs, will be resolved.