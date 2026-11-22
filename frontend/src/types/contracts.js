/** @typedef {{id:number, title:string, payload:object, author:string}} BusinessCase */
/** @typedef {{id:number, case_id:number, status:'queued'|'waiting'|'running'|'complete'|'failed', months?:string[], waiting_for?:string[]}} Run */
/** @typedef {{id:number, run_id:number, digest:string, report:{summary:string, metrics:object, sections:Array}}} Evidence */
