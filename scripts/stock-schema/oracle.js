// Runs the stock indexer's own query-limit libraries (@graphile/depth-limit 0.3.1 with
// maxDepth 9, graphql-query-complexity 1.0.0 with stock's estimators) on stock's schema, to
// check documents against crates/kdx-graphql/src/stock_limits.rs. Needs, as stock pins them:
//   npm i graphql@16.10.0 @graphql-tools/schema@10.0.6 @graphile/depth-limit@0.3.1 \
//     graphql-query-complexity@1.0.0
// Usage: node oracle.js <dir holding node_modules> <schema.graphql> <cases.json>
// where cases.json is [{"name", "q", "vars"?, "op"?, "expect"?}]; prints one JSON line per case.
// cases.json beside this script holds synthetic cross-check documents with this indexer's
// verdict in "expect" ({depth: refused by the depth rule, cx}); the oracle's "depth" (a
// non-empty list is a refusal) and "cx" should match it. The Rust test
// stock_limits::cases::the_cross_check_cases_hold keeps "expect" true to the port.
const path=require('path');const fs=require('fs');
const gen=process.argv[2];
const req=m=>require(path.join(gen,'node_modules',m));
const g=req('graphql');const {makeExecutableSchema}=req('@graphql-tools/schema');
const {depthLimit}=req('@graphile/depth-limit');
const {getComplexity,fieldExtensionsEstimator,directiveEstimator,simpleEstimator}=req('graphql-query-complexity');
const schema=makeExecutableSchema({typeDefs:fs.readFileSync(process.argv[3],'utf8')});
const cases=JSON.parse(fs.readFileSync(process.argv[4],'utf8'));
for(const c of cases){
  const doc=g.parse(c.q);
  const errs=g.validate(schema,doc,[depthLimit({maxDepth:9,revealDetails:false})]).map(e=>e.message);
  let cx;
  try{cx=getComplexity({schema,operationName:c.op,query:doc,variables:c.vars||{},estimators:[fieldExtensionsEstimator(),directiveEstimator({name:'complexity'}),simpleEstimator({defaultComplexity:0})]});}catch(e){cx='ERR '+e.message}
  const valid=g.validate(schema,doc).map(e=>e.message);
  console.log(JSON.stringify({name:c.name,depth:errs,cx,valid:valid.slice(0,2)}));
}
