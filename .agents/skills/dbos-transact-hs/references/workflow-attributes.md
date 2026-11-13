---
title: Tag Workflows with Attributes
impact: LOW
impactDescription: Unlabelled workflows cannot be found later
tags: workflow, attributes
---

## Tag Workflows with Attributes

`updateWorkflowAttributes` replaces a workflow's attributes, or clears them with `Nothing`. `mergeWorkflowAttributes` merges the top-level keys of an object into them. A key set to `null` is stored as `null` rather than removed. Attributes must be a JSON object.

```haskell
_ <- updateWorkflowAttributes dbos wid (Just "{\"team\":\"checkout\"}")
_ <- mergeWorkflowAttributes dbos wid "{\"region\":\"eu\"}"
```

The engine, in-workflow, and client variants share the shapes: `updateWorkflowAttributesInWorkflow` and `mergeWorkflowAttributesInWorkflow` record the change as a step of the calling workflow, and the `clientUpdateWorkflowAttributes` and `clientMergeWorkflowAttributes` calls act from outside. The encoding is checked before any step id is taken, so attributes that cannot be encoded are a call that never happened.
