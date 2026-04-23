# Thalia Studio

Thalia Studio is a rehearsal support system for stand-up comedy. It is designed to support an iterative workflow in which performers draft a bit, record a take, inspect moment-level feedback, revise the material, and try again.

Starting from a recorded rehearsal, the system highlights locally consequential moments in delivery, provides focused feedback grounded in the user’s own performance, and, when a prepared reference dataset is available, retrieves short video examples whose performance logic may help guide revision.

This repository contains the research prototype associated with our work on AI-supported stand-up comedy rehearsal. The project brings together writing support, rehearsal recording, performance analysis, and example-grounded revision within a single workflow.

## Overview

<p align="center">
  <img src="assets/images/teaser.png" alt="Thalia Studio teaser figure" width="950">
</p>

A bit can feel wrong during rehearsal without clearly revealing where the problem is. Thalia Studio is designed to make that moment visible and actionable. Instead of only giving broad comments on a full performance, the system localizes specific moments that may warrant coaching, explains why they matter, and supports revision through grounded comparison.

## Interface and Workflow

<p align="center">
  <img src="assets/images/UI.png" alt="Thalia Studio interface overview" width="950">
</p>

Thalia Studio supports an iterative rehearsal loop:

1. **Prepare for performance.** Users draft and organize material in the Writing Area.
2. **Record and analyze.** Users record a take and inspect a color-coded transcript that highlights matched, improvised, and missed material.
3. **Inspect moment-level feedback.** Users click a highlighted moment to view a focused delivery note, replay a local span, and study a retrieved reference example when available.
4. **Revise and try again.** Users return to the writing area to revise wording, order, or emphasis and rehearse again.

## Technical Pipeline

<p align="center">
  <img src="assets/images/method.png" alt="Thalia Studio technical pipeline" width="950">
</p>

The system is organized around three core components:

1. **Coaching target selection.** A recorded take is transcribed and segmented into short replayable spans. The system identifies moments whose local delivery may affect how the surrounding material is heard.
2. **Learnable reference span construction.** Long stand-up videos are converted into shorter reference spans that can function as teachable examples.
3. **Transferable example retrieval.** For each coaching target, the system retrieves a reference example whose performance logic is useful for revision.

## What This Repository Includes

This repository contains the research prototype, including:

- the web interface for writing and rehearsal
- the recording and transcript-based analysis flow
- the focused feedback and reference presentation logic
- the dataset preparation and indexing scripts used to support retrieval

## Important Note on Video Data

> **Important**
> Retrieved video references depend on a locally prepared reference dataset.
> The stand-up video corpus is **not distributed** with this repository.
> To enable retrieval, you must obtain the source videos yourself and run the preprocessing and indexing pipeline in advance.

This repository does not ship the stand-up video corpus used for retrieved references. The retrieval component depends on a prebuilt local reference database rather than raw videos alone. In practice, this means that retrieval will only work after the source videos have been prepared, processed into reference spans, and indexed locally.

If you skip dataset preparation, the writing and rehearsal parts of the application may still run depending on your local setup, but video-based reference retrieval will not be available.

## Preparing the Reference Dataset

To enable retrieved video examples, you must prepare the reference dataset yourself:

1. Obtain the source stand-up videos on your own.
2. Place the videos in the expected local data directory.
3. Run the preprocessing and indexing pipeline before launching retrieval-dependent features.

Depending on your setup, this may involve running ingestion, annotation, and reindexing scripts. For example:

```bash
python scripts/reindex_dataset_references.py
