const express = require('express');
const cors = require('cors');
const path = require('path');
const { spawn } = require('child_process');

const app = express();
const PORT = process.env.PORT || 3000;

// Get data directory from command line args
const dataDir = process.argv[2] || '/tmp';
console.log(`Serving Beech data from: ${dataDir}`);

// Path to beech-cli binary
const beechWriterPath = path.join(__dirname, '..', 'target', 'debug', 'beech-cli');

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Helper function to run beech-cli commands
function runBeechWriter(args) {
  return new Promise((resolve, reject) => {
    const child = spawn(beechWriterPath, args, {
      cwd: path.dirname(beechWriterPath)
    });
    
    let stdout = '';
    let stderr = '';
    
    child.stdout.on('data', (data) => {
      stdout += data.toString();
    });
    
    child.stderr.on('data', (data) => {
      stderr += data.toString();
    });
    
    child.on('close', (code) => {
      if (code === 0) {
        try {
          resolve(JSON.parse(stdout));
        } catch (e) {
          reject(new Error(`Failed to parse JSON: ${e.message}`));
        }
      } else {
        reject(new Error(`beech-cli exited with code ${code}: ${stderr}`));
      }
    });
  });
}

// API Routes

// Get tree info
app.get('/api/info', async (req, res) => {
  try {
    const info = await runBeechWriter(['info', '-d', dataDir]);
    res.json(info);
  } catch (error) {
    console.error('Error getting info:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get page details
app.get('/api/page/:pageId', async (req, res) => {
  try {
    const pageId = req.params.pageId;
    const pageInfo = await runBeechWriter(['inspect', '-d', dataDir, pageId]);
    res.json(pageInfo);
  } catch (error) {
    console.error('Error getting page info:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get tree structure for visualization
app.get('/api/tree-structure', async (req, res) => {
  try {
    const info = await runBeechWriter(['info', '-d', dataDir]);
    
    // Build tree structure for React Flow
    const nodes = [];
    const edges = [];
    
    // Add transaction root node
    const transactionNodeId = 'transaction-root';
    nodes.push({
      id: transactionNodeId,
      type: 'custom',
      position: { x: 0, y: 0 },
      data: {
        label: 'Transaction',
        pageId: info.transaction_id.substring(0, 8) + '...',
        pageType: 'transaction',
        numKeys: info.tables.length,
        numRows: null,
        tableName: null,
        transactionDate: info.transaction_date
      }
    });
    
    // Add table nodes and connect to transaction
    let tableX = -(info.tables.length - 1) * 150;
    for (const table of info.tables) {
      const tableNodeId = `table-${table.id}`;
      
      // Add table node
      nodes.push({
        id: tableNodeId,
        type: 'custom',
        position: { x: tableX, y: 150 },
        data: {
          label: `Table: ${table.name}`,
          pageId: table.id.substring(0, 8) + '...',
          pageType: 'table',
          numKeys: table.columns,
          numRows: table.total_rows,
          tableName: table.name
        }
      });
      
      // Add edge from transaction to table
      edges.push({
        id: `${transactionNodeId}-${tableNodeId}`,
        source: transactionNodeId,
        target: tableNodeId,
        type: 'smoothstep'
      });
      
      // Build tree for this table's root page
      if (table.root_node) {
        await buildTreeNodes(table.root_node, nodes, edges, tableX, 300, table.name, tableNodeId, 0);
      }
      
      tableX += 300;
    }
    
    res.json({ nodes, edges });
  } catch (error) {
    console.error('Error building tree structure:', error);
    res.status(500).json({ error: error.message });
  }
});

// Recursive function to build tree nodes
async function buildTreeNodes(pageId, nodes, edges, x, y, tableName, parentId = null, level = 0) {
  try {
    const pageInfo = await runBeechWriter(['inspect', '-d', dataDir, pageId]);
    
    // Add this node
    nodes.push({
      id: pageId,
      type: 'custom',
      position: { x: x, y: y },
      data: {
        label: `${pageInfo.node_type} Page`,
        pageId: pageId.substring(0, 8) + '...',
        pageType: pageInfo.node_type,
        numKeys: pageInfo.num_keys,
        numRows: pageInfo.num_rows,
        tableName: level === 0 ? tableName : undefined
      }
    });
    
    // Add edge from parent if exists
    if (parentId) {
      edges.push({
        id: `${parentId}-${pageId}`,
        source: parentId,
        target: pageId,
        type: 'smoothstep'
      });
    }
    
    // Process children if this is a branch node
    if (pageInfo.children && pageInfo.children.length > 0) {
      const childSpacing = 200;
      const startX = x - (pageInfo.children.length - 1) * childSpacing / 2;
      
      for (let i = 0; i < pageInfo.children.length; i++) {
        const childId = pageInfo.children[i];
        const childX = startX + i * childSpacing;
        const childY = y + 150;
        
        await buildTreeNodes(childId, nodes, edges, childX, childY, tableName, pageId, level + 1);
      }
    }
  } catch (error) {
    console.error(`Error processing page ${pageId}:`, error);
  }
}

// Serve the main page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Beech Visualizer running on http://localhost:${PORT}`);
  console.log(`Data directory: ${dataDir}`);
});