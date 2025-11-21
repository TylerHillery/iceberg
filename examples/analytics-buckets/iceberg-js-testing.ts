import { IcebergRestCatalog } from 'iceberg-js'

const SUPABASE_WAREHOUSE = process.env.SUPABASE_WAREHOUSE!
const SUPABASE_TOKEN = process.env.SUPABASE_TOKEN!
const SUPABASE_CATALOG_URI = process.env.SUPABASE_CATALOG_URI!

const catalog = new IcebergRestCatalog({
  baseUrl: SUPABASE_CATALOG_URI,
  catalogName: SUPABASE_WAREHOUSE,
  auth: {
    type: 'bearer',
    token: SUPABASE_TOKEN,
  },
  accessDelegation: ['vended-credentials'],
})

async function listAll() {
  let namespaces = await catalog.listNamespaces()

  for (const namespaceItem of namespaces) {
    let namespaceName = namespaceItem.namespace[0]
    console.log(namespaceName)
    let tables = await catalog.listTables(namespaceItem)
    for (let i = 0; i < tables.length; i++) {
      const isLast = i === tables.length - 1
      const prefix = isLast ? '└── ' : '├── '
      console.log(prefix + tables[i]!.name)
    }
  }
}

async function createTable(namespace: any, tableName: string) {
  const metadata = await catalog.createTable(
    namespace,
    {
      name: tableName,
      schema: {
        type: 'struct',
        fields: [
          { id: 1, name: 'VendorID', type: 'long', required: false },
          { id: 2, name: 'tpep_pickup_datetime', type: 'timestamp', required: false },
          { id: 3, name: 'tpep_dropoff_datetime', type: 'timestamp', required: false },
          { id: 4, name: 'passenger_count', type: 'double', required: false },
          { id: 5, name: 'trip_distance', type: 'double', required: false },
          { id: 6, name: 'RatecodeID', type: 'double', required: false },
          { id: 7, name: 'store_and_fwd_flag', type: 'string', required: false },
          { id: 8, name: 'PULocationID', type: 'long', required: false },
          { id: 9, name: 'DOLocationID', type: 'long', required: false },
          { id: 10, name: 'payment_type', type: 'long', required: false },
          { id: 11, name: 'fare_amount', type: 'double', required: false },
          { id: 12, name: 'extra', type: 'double', required: false },
          { id: 13, name: 'mta_tax', type: 'double', required: false },
          { id: 14, name: 'tip_amount', type: 'double', required: false },
          { id: 15, name: 'tolls_amount', type: 'double', required: false },
          { id: 16, name: 'improvement_surcharge', type: 'double', required: false },
          { id: 17, name: 'total_amount', type: 'double', required: false },
          { id: 18, name: 'congestion_surcharge', type: 'double', required: false },
          { id: 19, name: 'airport_fee', type: 'double', required: false },
        ],
        'schema-id': 0,
        'identifier-field-ids': [],
      },
      'partition-spec': {
        'spec-id': 0,
        fields: [],
      },
      'write-order': {
        'order-id': 0,
        fields: [],
      },
      'stage-create': false,
      properties: {},
    }
  )
}

console.log('\n═══════════════════════════════════════')
console.log('           🚀 STARTING')
console.log('═══════════════════════════════════════\n')
// createNamespaces
await listAll()

const newNamespace = { namespace: ['test'] }
const newTable = "taxi_dataset"

await catalog.createNamespace(newNamespace, { properties: { owner: 'data-team' } })
console.log('\n✅ ADDED NEW NAMESPACE\n')
await listAll()

await createTable(newNamespace, newTable)
console.log('\n✅ ADDED NEW TABLE\n')
await listAll()

await catalog.dropTable({ ...newNamespace, name: newTable })
console.log('\n❌ DROPPED NEW TABLE\n')
await listAll()

await catalog.dropNamespace(newNamespace)
console.log('\n❌ DROPPED NEW NAMESPACE\n')
await listAll()
