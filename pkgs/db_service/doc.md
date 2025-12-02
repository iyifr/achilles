Let's tweak it to possibly allow so

So filters are an object with kv pairs

Keys are expected to match keys in some emtadata structs

if the value is a string, int, float, double or array, we process it directly as a value to compare for equality

If it's an object and any of the keys are special

Query
{
tenantId: {"$in": ArrayVal}
name: "vamos-tx"
}
valQueries = []
objectQueries = []

for key, value in filters:
if typeof(value) == string || typeof(value) == int || typeof(value) == float || typeof(value) == array:
valQueries.append({key: key, value: value, type: "equality"})
elif typeof(value) == object:
objectQueries.append({key: key, value: value, type: "object"})
// Process direct value queries - batch approach
allMatchingDocs = getAllDocuments() // Start with all documents

// Single pass through all documents checking all conditions at once
filteredDocs = []
for doc in allMatchingDocs:
matchesAll = true
for query in valQueries:
if doc.metadata[query.key] != query.value:
matchesAll = false
break
if matchesAll:
filteredDocs.append(doc)

allMatchingDocs = filteredDocs

// Process object queries (like $in, $gt, etc.)
for query in objectQueries:
// Handle special operators like $in, $gt, $lt, etc.
matchingDocs = searchByObjectQuery(query.key, query.value)

for result in results {

}
