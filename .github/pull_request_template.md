### Description





<br />

---

### Submitter

<details>
  <summary><strong>General</strong></summary>

- The PR addresses a **single thing** (at most ~300 LOC)
- The PR includes relevant integration, e2e or unit **tests** (unless dealing with emergency)
- The system **will continue to work** well for its users and for the developers after the PR is merged
- Related **documentation** was updated
- The PR contains some contextual description of the submitted feature (screenshot or video for UI changes)

</details>



### Reviewer

<details>
  <summary><strong>General</strong></summary>
  
  _Instead of seeking perfection, seek continuous improvement._ A PR that,
as a whole, improves the maintainability, readability, and understandability
of the system shouldn’t be delayed for days or weeks because it isn’t “perfect.”

  
  - The intention of the PR **is understood** (via description, UI screenshot or video, etc)
  - The code behaves as the author likely intended and the way the code behaves is good for its users.
  - The PR addresses a **single thing** (at most ~300 LOC)
  - The PR includes related integration, e2e or unit **tests** (unless dealing with emergency)
  - The PR **doesn't introduce a breaking change**. The system will continue to work well for its users and for the developers after the PR is merged
  - The PR is in a state where it definitely **improves the overall code health** of the system being worked on, _even if the PR isn’t perfect_
  - Related **documentation** was updated (incl. deprecation notice, etc)
</details>


<details>
  <summary><strong>Design</strong></summary>
  
  - Is now **a good time** to add this functionality?
  - Does it integrate well with the rest of your system?
  - Does this change belong in your codebase, or in a library?
  - Did the developer pick **good names** for everything?
  - Are the comments clear and useful, and mostly explain _why_ instead of what?
  - Does the changed code need refactoring? _(e.g. added few lines to function, but now it needs be broken up into smaller functions)_
</details>


<details>
  <summary><strong>Functionality & Complexity</strong></summary>
  
  - Is the code **too complex**? _(e.g. can’t be understood quickly by code readers; developers are likely to introduce bugs when they try to call or modify this code)_
  - Is the code over-engineered? Is there added **functionality that isn’t presently needed** by the system?
  - Are potential race conditions and deadlocks handled well?
</details>


<details>
  <summary><strong>Tests</strong></summary>
  
  _Tests are also code that has to be maintained._
Don’t accept complexity in tests just because they aren’t part of app in production.


  - Will the tests **actually fail** when the code is broken?
  - If the code changes beneath them, will they start producing **false positives**?
  - Does each test make simple and useful assertions?
  - Are the tests separated appropriately between different test methods?

</details>

<details>
  <summary><strong>Context</strong></summary>
  
  - Is this PR improving the code health of the system? Or is it making the whole system more complex, less tested, etc.? 

</details>

<details>
  <summary><strong>How-tos</strong></summary>
  
  - Comments that signify unimportant notes are prefixed with `Nit:` (short for _nitpicking_) or similar.
- Don't forget to offer encouragement and appreciation for good practices. It’s sometimes even more valuable, in terms of mentoring, to tell a developer what they did right than to tell them what they did wrong.


  - Be kind.
  - Explain your reasoning.
  - Balance giving explicit directions with just pointing out problems and letting the developer decide.
  - Encourage the submitter to simplify code or add code comments instead of just explaining the complexity to you.

</details>