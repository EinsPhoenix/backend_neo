// Globale Variablen
let camera, scene, renderer, raycaster;
let nodes = new Map();
let relationships = [];
let nodeObjects = new Map();
let nodeLabels = new Map(); 
let lineObjects = [];
let lineLabels = [];      
let hoveredObject = null;
let infoPanel;
let loadingIndicator;
let fileInput;
let dropZone;
let jsonData = null;
let cameraController;

let selectedObject = null;

let disableAnimations = false;

let disableNiceMeshes = false;

let labelColorMap = new Map();


const neonColors = [
    0xff00ff, // Magenta
    0x00ffff, // Cyan
    0xff0066, // Pink
    0x00ff00, // Grün
    0xff3300, // Orange
    0x9900ff, // Lila
    0x00ccff, // Hellblau
    0xffff00, // Gelb
    0xff0000, // Rot
    0x0000ff  // Blau
];
let nextColorIndex = 0;



let composer, bloomPass;


const SPACE_SIZE = 50000;


const MIN_DISTANCE = 100;


function init() {
   
    infoPanel = document.getElementById('info');
    loadingIndicator = document.getElementById('loading');
    fileInput = document.getElementById('fileInput');
    dropZone = document.getElementById('dropZone');
    
    
    document.getElementById('selectFileBtn').addEventListener('click', () => fileInput.click());
    fileInput.addEventListener('change', handleFileSelect);
    
   
    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.style.borderColor = '#4CAF50';
    });
    
    dropZone.addEventListener('dragleave', () => {
        dropZone.style.borderColor = '#ccc';
    });
    
    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.style.borderColor = '#ccc';
        
        if (e.dataTransfer.files.length) {
            handleFile(e.dataTransfer.files[0]);
        }
    });

    // Three.js Setup - create these FIRST
    scene = new THREE.Scene();
    scene.background = new THREE.Color(0x000000);
    
    camera = new THREE.PerspectiveCamera(60, window.innerWidth / window.innerHeight, 1, 100000);
    camera.position.set(0, 0, 1000);
    camera.lookAt(0, 0, 0);
    
    renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(window.innerWidth, window.innerHeight);
    document.body.appendChild(renderer.domElement);
    
   
    renderer.domElement.addEventListener('click', handleClick);
    
    raycaster = new THREE.Raycaster();
    
 
    cameraController = new CameraController(camera, renderer.domElement);
    
   
    window.addEventListener('resize', onWindowResize);
    
  
    const groundLight = addGroundLightCircle();
    scene.add(groundLight);
    

    const ambientLight = new THREE.AmbientLight(0xffffff, 0.5);
    ambientLight.position.set(0, 25000, 0);
    scene.add(ambientLight);
    
 
    const pointLight = new THREE.PointLight(0xffffff, 1);
    pointLight.position.set(0, 25000, 0);
    scene.add(pointLight);
    

    try {
        setupBloomEffect();
    } catch (error) {
        console.error("Konnte Bloom-Effekt nicht initialisieren:", error);
     
    }
    
 
    setupGUI();
    
    
    animate();
}


function setupGUI() {
const gui = new dat.GUI();


const params = {
renderOption: 'Standard',
};


gui.add(params, 'renderOption', ['Qualität', 'Standard', 'Performance']).name('Anzeigemodus').onChange(function(value) {
applyRenderOption(value);
});


applyRenderOption('Standard');
}


function applyRenderOption(option) {
switch(option) {
case 'Qualität':
 
    if (composer && bloomPass) {
        bloomPass.strength = 1.25;
        bloomPass.radius = 0.6;
        bloomPass.threshold = 0.85;
    }
    
  
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    
    
    scene.children.forEach(child => {
        if (child.geometry instanceof THREE.CircleGeometry) {
            child.visible = true;
        }
    });
    
 
    nodeLabels.forEach(label => {
        label.visible = true;
    });
    
    lineLabels.forEach(labelInfo => {
        labelInfo.sprite.visible = true;
    });
    
   
    disableAnimations = false;

    disableNiceMeshes = false;
    
   
    loadingIndicator.textContent = 'Qualitätsmodus: Bloom, Schatten, Ground Circle und Labels aktiviert';
    break;
    
case 'Standard':

    if (composer && bloomPass) {
        bloomPass.strength = 0;
        bloomPass.radius = 0;
        bloomPass.threshold = 1;
    }
    
   
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    
    
    scene.children.forEach(child => {
        if (child.geometry instanceof THREE.CircleGeometry) {
            child.visible = true;
        }
    });
    
 
    nodeLabels.forEach(label => {
        label.visible = true;
    });
    
    lineLabels.forEach(labelInfo => {
        labelInfo.sprite.visible = true;
    });
    
  
    disableAnimations = false;
    disableNiceMeshes = false;
    
   
    loadingIndicator.textContent = 'Standardmodus: Bloom deaktiviert, Rest aktiviert';
    break;
    
case 'Performance':
  
    if (composer && bloomPass) {
        bloomPass.strength = 0;
        bloomPass.radius = 0;
        bloomPass.threshold = 1;
    }

    renderer.shadowMap.enabled = false;
    
    
    scene.children.forEach(child => {
        if (child.geometry instanceof THREE.CircleGeometry) {
            child.visible = false;
        }
    });
    
 
    nodeLabels.forEach(label => {
        label.visible = false;
    });
    
    lineLabels.forEach(labelInfo => {
        labelInfo.sprite.visible = false;
    });
    
 
    disableAnimations = true;

    disableNiceMeshes = true;
    
   
    loadingIndicator.textContent = 'Performance-Modus: Alle Effekte und Animationen deaktiviert für maximale Geschwindigkeit';
    break;
}
}

function handleClick(event) {

if (event.button !== 0) return;

const mouse = cameraController.getMouseCoordinates();
raycaster.setFromCamera(mouse, camera);

const objects = [...nodeObjects.values(), ...lineObjects];
const intersects = raycaster.intersectObjects(objects);

if (intersects.length > 0) {
const object = intersects[0].object;


selectedObject = object;


if (object.userData.type === 'node') {
    object.material.emissiveIntensity = 2.0;
    object.scale.set(1.2, 1.2, 1.2);
} else {
    object.material.opacity = 1.0;
}


if (hoveredObject && hoveredObject !== selectedObject) {
    if (hoveredObject.userData.type === 'node') {
        hoveredObject.material.emissiveIntensity = 0.7;
        hoveredObject.scale.set(1, 1, 1);
    } else {
        hoveredObject.material.opacity = 0.6;
    }
}

hoveredObject = null;
updateInfoPanel(object.userData);
infoPanel.style.display = 'block';
} else {

if (selectedObject) {
    
    if (selectedObject.userData.type === 'node') {
        selectedObject.material.emissiveIntensity = 0.7;
        selectedObject.scale.set(1, 1, 1);
    } else {
        selectedObject.material.opacity = 0.6;
    }
    selectedObject = null;
}
infoPanel.style.display = 'none';
}
}


function clearSelection() {
if (selectedObject) {

if (selectedObject.userData.type === 'node') {
    selectedObject.material.emissiveIntensity = 0.7;
    selectedObject.scale.set(1, 1, 1);
} else {
    selectedObject.material.opacity = 0.6;
}
}
selectedObject = null;
infoPanel.style.display = 'none';
}

// Bloom-Effekt Setup
function setupBloomEffect() {
    try {
      
        if (typeof THREE.EffectComposer === 'undefined' || 
            typeof THREE.RenderPass === 'undefined' || 
            typeof THREE.UnrealBloomPass === 'undefined') {
            
            console.warn('Bloom effect could not be initialized. Required dependencies missing.');
            composer = null;
            return;
        }
        
      
        composer = new THREE.EffectComposer(renderer);
        
        const renderPass = new THREE.RenderPass(scene, camera);
        composer.addPass(renderPass);
        
      
        const bloomLayer = new THREE.Layers();
        bloomLayer.set(0); 
        
     
        bloomPass = new THREE.UnrealBloomPass(
            new THREE.Vector2(window.innerWidth, window.innerHeight),
            1.25,  
            0.6,   
            0.85   
        );
        
       
        composer.addPass(bloomPass);
        
 
        const bloomComposer = new THREE.EffectComposer(renderer);
        const bloomRenderPass = new THREE.RenderPass(scene, camera);
        
        bloomRenderPass.clear = true;
        
        // Add pre-filter to exclude objects on layer 1
        const darkenNonBloomed = new THREE.ShaderPass(
            new THREE.ShaderMaterial({
                uniforms: {
                    tDiffuse: { value: null }
                },
                vertexShader: `
                    varying vec2 vUv;
                    void main() {
                        vUv = uv;
                        gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
                    }
                `,
                fragmentShader: `
                    uniform sampler2D tDiffuse;
                    varying vec2 vUv;
                    void main() {
                        vec4 color = texture2D(tDiffuse, vUv);
                        gl_FragColor = color;
                    }
                `,
                defines: {}
            })
        );
        
        // Set up the final composer
        console.log("Bloom effect successfully initialized");
    } catch (error) {
        console.error("Error initializing bloom effect:", error);
        composer = null;
    }
}

// Event-Handler für Dateiauswahl
function handleFileSelect(event) {
    if (event.target.files.length) {
        handleFile(event.target.files[0]);
    }
}

// Datei verarbeiten
function handleFile(file) {
    if (file.type !== 'application/json') {
        alert('Bitte wähle eine JSON-Datei aus.');
        return;
    }
    
    dropZone.style.display = 'none';
    loadingIndicator.textContent = 'Lade JSON-Daten...';
    
    const reader = new FileReader();
    reader.onload = function(e) {
        try {
            jsonData = JSON.parse(e.target.result);
            processDataWithWorker(jsonData);
        } catch (error) {
            loadingIndicator.textContent = 'Fehler beim Parsen der JSON-Datei';
            console.error('JSON Parse Error:', error);
        }
    };
    reader.readAsText(file);
}

// Worker für Multithreading
function processDataWithWorker(jsonData) {
    loadingIndicator.textContent = 'Verarbeite Daten in einem separaten Thread...';
    
    // Web Worker erstellen
    const workerBlob = new Blob([`
        self.onmessage = function(e) {
            const data = e.data;
            const processedData = processData(data);
            self.postMessage(processedData);
        };
        
        function processData(data) {
            const nodes = new Map();
            const relationships = [];
            const SPACE_SIZE = 50000;
            const MIN_DISTANCE = 100;
            
            // Nodes extrahieren
            data.forEach(entry => {
                entry.forEach(item => {
                    if (item.type === 'node' && !nodes.has(item.id)) {
                        nodes.set(item.id, {
                            id: item.id,
                            labels: item.labels,
                            properties: item.properties,
                            position: getRandomPosition(SPACE_SIZE),
                            connections: 0
                        });
                    } else if (item.type === 'relationship') {
                        relationships.push({
                            id: item.id,
                            label: item.label,
                            startId: item.start.id,
                            endId: item.end.id
                        });
                        
                        // Verbindungszähler erhöhen
                        if (nodes.has(item.start.id)) {
                            nodes.get(item.start.id).connections++;
                        }
                        if (nodes.has(item.end.id)) {
                            nodes.get(item.end.id).connections++;
                        }
                    }
                });
            });
            
            // Zufallsposition generieren
            function getRandomPosition(size) {
                return {
                    x: (Math.random() - 0.5) * size,
                    y: (Math.random() - 0.5) * size,
                    z: (Math.random() - 0.5) * size
                };
            }
            
            // Überlappungen vermeiden mit Simple Physics
            const nodePositions = Array.from(nodes.values());
            resolveCollisions(nodePositions);
            
            // Einfacher Algorithmus zur Kollisionsvermeidung
            function resolveCollisions(nodeList) {
                const iterations = 10;
                
                for (let iter = 0; iter < iterations; iter++) {
                    for (let i = 0; i < nodeList.length; i++) {
                        for (let j = i + 1; j < nodeList.length; j++) {
                            const node1 = nodeList[i];
                            const node2 = nodeList[j];
                            
                            const dx = node2.position.x - node1.position.x;
                            const dy = node2.position.y - node1.position.y;
                            const dz = node2.position.z - node1.position.z;
                            
                            const distance = Math.sqrt(dx*dx + dy*dy + dz*dz);
                            
                            // Größe basierend auf Verbindungen
                            const size1 = 5 + Math.min(45, node1.connections * 5);
                            const size2 = 5 + Math.min(45, node2.connections * 5);
                            
                            const minDist = MIN_DISTANCE + size1/2 + size2/2;
                            
                            if (distance < minDist) {
                                // Abstoßungsvektor berechnen
                                const force = (minDist - distance) / distance * 0.5;
                                
                                node1.position.x -= dx * force;
                                node1.position.y -= dy * force;
                                node1.position.z -= dz * force;
                                
                                node2.position.x += dx * force;
                                node2.position.y += dy * force;
                                node2.position.z += dz * force;
                            }
                        }
                    }
                }
                
                // Sicherstellen, dass alle Nodes im definierten Raum bleiben
                for (const node of nodeList) {
                    const halfSize = SPACE_SIZE / 2;
                    node.position.x = Math.max(-halfSize, Math.min(halfSize, node.position.x));
                    node.position.y = Math.max(-halfSize, Math.min(halfSize, node.position.y));
                    node.position.z = Math.max(-halfSize, Math.min(halfSize, node.position.z));
                }
            }
            
            // Ergebnis als Objekt zurückgeben
            return {
                nodes: Array.from(nodes.entries()),
                relationships: relationships
            };
        }
    `], { type: 'application/javascript' });
    
    const workerUrl = URL.createObjectURL(workerBlob);
    const worker = new Worker(workerUrl);
    
    worker.onmessage = function(e) {
        const processedData = e.data;
        
        
        nodes = new Map(processedData.nodes);
        relationships = processedData.relationships;
        
      
        createVisualization();
        
        
        worker.terminate();
        URL.revokeObjectURL(workerUrl);
        
        loadingIndicator.textContent = `${nodes.size} Nodes und ${relationships.length} Beziehungen geladen`;
    };
    
    worker.onerror = function(error) {
        console.error('Worker error:', error);
        loadingIndicator.textContent = 'Fehler bei der Datenverarbeitung';
    };
    
   
    worker.postMessage(jsonData);
}

function createVisualization() {
  
    const batchSize = 50;
    let nodeCount = 0;
    let processedCount = 0;
    

    nodeCount = nodes.size;
    
  
    function createNodesInBatches() {
        const nodesToProcess = Array.from(nodes.entries())
            .slice(processedCount, processedCount + batchSize);
        
        nodesToProcess.forEach(([id, nodeData]) => {
            createNode(id, nodeData);
            processedCount++;
        });
        
        loadingIndicator.textContent = `Lade Nodes: ${processedCount}/${nodeCount}`;
        
        if (processedCount < nodeCount) {
          
            setTimeout(createNodesInBatches, 10);
        } else {
            
            createRelationshipsInBatches();
        }
    }
  
    function createRelationshipsInBatches() {
        const totalRelationships = relationships.length;
        let processedRelationships = 0;
        
        function processBatch() {
            const batchSize = 50;
            const relToProcess = relationships.slice(
                processedRelationships, 
                processedRelationships + batchSize
            );
            
            relToProcess.forEach(rel => {
                createRelationship(rel);
                processedRelationships++;
            });
            
            loadingIndicator.textContent = `Lade Beziehungen: ${processedRelationships}/${totalRelationships}`;
            
            if (processedRelationships < totalRelationships) {
                setTimeout(processBatch, 10);
            } else {
                loadingIndicator.textContent = `Visualisierung komplett: ${nodeCount} Nodes, ${totalRelationships} Beziehungen`;
                setTimeout(() => {
                    loadingIndicator.style.opacity = '0.5';
                }, 3000);
            }
        }
        
        processBatch();
    }
    
   
    createNodesInBatches();
}

// Text-Label erstellen
function createTextLabel(text, color = 0xffffff) {
   
    const canvas = document.createElement('canvas');
    const context = canvas.getContext('2d');
    canvas.width = 256;
    canvas.height = 64;
    
   
    context.fillStyle = 'rgba(0, 0, 0, 0)';
    context.fillRect(0, 0, canvas.width, canvas.height);
    
  
    context.font = 'Bold 24px Arial';
    context.fillStyle = `#${color.toString(16).padStart(6, '0')}`;
    context.textAlign = 'center';
    context.textBaseline = 'middle';
    

    let displayText = text;
    if (text.length > 20) {
        displayText = text.slice(0, 18) + '...';
    }
    
    context.fillText(displayText, canvas.width / 2, canvas.height / 2);
    

    const texture = new THREE.CanvasTexture(canvas);
    texture.needsUpdate = true;
    
    const spriteMaterial = new THREE.SpriteMaterial({ 
        map: texture, 
        transparent: true 
    });
    
    const sprite = new THREE.Sprite(spriteMaterial);
    sprite.scale.set(200, 50, 1);
    
    return sprite;
}


function getColorForLabel(label) {
  
    if (labelColorMap.has(label)) {
        return labelColorMap.get(label);
    }
    

    let color;
    
    if (nextColorIndex < neonColors.length) {
      
        color = neonColors[nextColorIndex];
        nextColorIndex++;
    } else {
    
        const baseColor = neonColors[nextColorIndex % neonColors.length];
        
     
        const variation = 0.2;
        const r = ((baseColor >> 16) & 0xff) / 255;
        const g = ((baseColor >> 8) & 0xff) / 255;
        const b = (baseColor & 0xff) / 255;
        
        const rNew = Math.max(0, Math.min(1, r + (Math.random() * variation * 2 - variation)));
        const gNew = Math.max(0, Math.min(1, g + (Math.random() * variation * 2 - variation)));
        const bNew = Math.max(0, Math.min(1, b + (Math.random() * variation * 2 - variation)));
        
        color = (Math.floor(rNew * 255) << 16) | 
                (Math.floor(gNew * 255) << 8) | 
                Math.floor(bNew * 255);
        
        nextColorIndex++;
    }
    
   
    labelColorMap.set(label, color);
    
    return color;
}

// Node als 3D-Objekt erstellen
function createNode(id, nodeData) {
    const size = 20 + Math.min(70, nodeData.connections * 5);
    const innerSize = size * 0.8;

    const innerGeometry = new THREE.SphereGeometry(innerSize, 32, 32);
    const primaryLabel = nodeData.labels[0] || `Node ${id}`;
    const color = getColorForLabel(primaryLabel);

    const innerMaterial = new THREE.MeshStandardMaterial({
        color: color,
        emissive: color,
        emissiveIntensity: 0.5,
        metalness: 0.9,
        roughness: 0.7
    });

    const innerMesh = new THREE.Mesh(innerGeometry, innerMaterial);
    innerMesh.position.set(0, 0, 0);

    let finalMesh = innerMesh;

    if (!disableNiceMeshes) {
        const outerGeometry = new THREE.SphereGeometry(size, 32, 32);
        const outerMaterial = new THREE.MeshStandardMaterial({
            color: color,
            emissive: color,
            emissiveIntensity: 0.5,
            transparent: true,
            opacity: 0.5,
            metalness: 0.9,
            roughness: 0.7
        });

        const outerMesh = new THREE.Mesh(outerGeometry, outerMaterial);
        outerMesh.position.set(
            nodeData.position.x,
            nodeData.position.y,
            nodeData.position.z
        );

        outerMesh.add(innerMesh);
        finalMesh = outerMesh;
    } else {
        innerMesh.position.set(
            nodeData.position.x,
            nodeData.position.y,
            nodeData.position.z
        );
    }

    finalMesh.userData = {
        id: id,
        labels: nodeData.labels,
        properties: nodeData.properties,
        type: 'node'
    };

    scene.add(finalMesh);
    nodeObjects.set(id, finalMesh);

    const labelText = primaryLabel;
    const labelSprite = createTextLabel(labelText, color);
    labelSprite.position.set(
        nodeData.position.x,
        nodeData.position.y + size + 30,
        nodeData.position.z
    );

    scene.add(labelSprite);
    nodeLabels.set(id, labelSprite);
}



function createRelationship(relationship) {
    const startNode = nodeObjects.get(relationship.startId);
    const endNode = nodeObjects.get(relationship.endId);
    
    if (!startNode || !endNode) {
        console.warn('Beziehung mit fehlendem Node:', relationship);
        return;
    }
    
 
    const points = [
        startNode.position.clone(),
        endNode.position.clone()
    ];
    
    const geometry = new THREE.BufferGeometry().setFromPoints(points);
    

    const material = new THREE.LineBasicMaterial({
        color: 0xffffff,
        transparent: true,
        opacity: 0.2
    });
    
  
    const line = new THREE.Line(geometry, material);
    

    line.userData = {
        id: relationship.id,
        label: relationship.label,
        startId: relationship.startId,
        endId: relationship.endId,
        type: 'relationship'
    };
    

    scene.add(line);
    lineObjects.push(line);
    
  
    const labelText = relationship.label || `Rel ${relationship.id}`;
    const labelSprite = createTextLabel(labelText, 0xffff00); 
    

    const midPoint = new THREE.Vector3().addVectors(
        startNode.position,
        endNode.position
    ).multiplyScalar(0.5);
    
  
    midPoint.y += 20;
    
    labelSprite.position.copy(midPoint);
    
 
    scene.add(labelSprite);
    lineLabels.push({
        sprite: labelSprite,
        startId: relationship.startId,
        endId: relationship.endId
    });
}



function onWindowResize() {
    camera.aspect = window.innerWidth / window.innerHeight;
    camera.updateProjectionMatrix();
    renderer.setSize(window.innerWidth, window.innerHeight);
    

    if (composer && composer.setSize) {
        try {
            composer.setSize(window.innerWidth, window.innerHeight);
        } catch (error) {
            console.error("Fehler beim Resize des Composers:", error);
        }
    }
}


function checkHover() {

if (selectedObject) return;

const mouse = cameraController.getMouseCoordinates();

raycaster.setFromCamera(mouse, camera);


const objects = [...nodeObjects.values(), ...lineObjects];
const intersects = raycaster.intersectObjects(objects);

if (intersects.length > 0) {
const object = intersects[0].object;

if (hoveredObject !== object) {
  
    if (hoveredObject) {
        if (hoveredObject.userData.type === 'node') {
            hoveredObject.material.emissiveIntensity = 0.7;
            hoveredObject.scale.set(1, 1, 1);
        } else {
            hoveredObject.material.opacity = 0.6;
        }
    }

    if (object.userData.type === 'node') {
        object.material.emissiveIntensity = 1.5;
        object.scale.set(1.1, 1.1, 1.1);
    } else {
        object.material.opacity = 1.0;
    }
    
    updateInfoPanel(object.userData);
    
    hoveredObject = object;
}

infoPanel.style.display = 'block';
} else {

if (hoveredObject) {
    if (hoveredObject.userData.type === 'node') {
        hoveredObject.material.emissiveIntensity = 0.7;
        hoveredObject.scale.set(1, 1, 1);
    } else {
        hoveredObject.material.opacity = 0.6;
    }
    
    hoveredObject = null;
    infoPanel.style.display = 'none';
}
}
}


function updateInfoPanel(data) {
let html = '';

if (data.type === 'node') {
html += `<strong>Node:</strong> ${data.labels.join(', ')}<br>`;
html += `<strong>ID:</strong> ${data.id}<br><br>`;

if (data.properties) {
    html += '<strong>Properties:</strong><br>';
    for (const [key, value] of Object.entries(data.properties)) {
        html += `${key}: ${value}<br>`;
    }
}
} else if (data.type === 'relationship') {
html += `<strong>Relationship:</strong> ${data.label}<br>`;
html += `<strong>ID:</strong> ${data.id}<br>`;
html += `<strong>From:</strong> ${data.startId}<br>`;
html += `<strong>To:</strong> ${data.endId}<br>`;
}

const closeButton = `<button class="btn" 
style="position: absolute; top: 5px; right: 5px; padding: 2px 8px; font-size: 12px;"
onclick="clearSelection()">X</button>`;

html += closeButton;

infoPanel.innerHTML = html;
}


function updateLabels() {

for (const [nodeId, label] of nodeLabels.entries()) {
const node = nodeObjects.get(nodeId);
if (node) {
  
    label.lookAt(camera.position);
    
   
    const distance = camera.position.distanceTo(node.position);
    
  
    let scale = 1;
    
    if (distance > 5000) {
      
        scale = 1;
    } else if (distance > 2500) {
       
        const progress = (5000 - distance) / 2500; 
        scale = 0.1 + (5 * progress); 
    } else if (distance > 100) {
     
        const progress = (2500 - distance) / 2000; 
        scale = 5 - (4 * progress); 
    } else {
        
        scale = 1;
    }
    
  
    label.scale.set(200 * scale, 50 * scale, 1);
}
}

// Beziehungs-Labels aktualisieren
for (const labelInfo of lineLabels) {
const startNode = nodeObjects.get(labelInfo.startId);
const endNode = nodeObjects.get(labelInfo.endId);

if (startNode && endNode) {
  
    const midPoint = new THREE.Vector3().addVectors(
        startNode.position,
        endNode.position
    ).multiplyScalar(0.5);
    
  
    midPoint.y += 20;
    
    labelInfo.sprite.position.copy(midPoint);

    labelInfo.sprite.lookAt(camera.position);
    

    const distance = camera.position.distanceTo(midPoint);
    

    let scale = 1; 
    
    if (distance > 5000) {
     
        scale = 1;
    } else if (distance > 2500) {
        
        const progress = (5000 - distance) / 2500; 
        scale = 0.1 + (2 * progress); 
    } else if (distance > 100) {
      
        const progress = (2500 - distance) / 2000; 
        scale = 2 - (1 * progress); 
    } else {
        
        scale = 1;
    }
    
    
    labelInfo.sprite.scale.set(200 * scale, 50 * scale, 1);
}
}
}
function addGroundLightCircle() {

const canvas = document.createElement('canvas');
canvas.width = 1024;
canvas.height = 1024;
const context = canvas.getContext('2d');

const gradient = context.createRadialGradient(
canvas.width / 2, // x0
canvas.height / 2, // y0
0, // r0
canvas.width / 2, // x1
canvas.height / 2, // y1
canvas.width / 2 // r1
);

gradient.addColorStop(0, 'rgba(255, 255, 255, 1)');   
gradient.addColorStop(0.5, 'rgba(255, 255, 255, 0.5)'); 
gradient.addColorStop(1, 'rgba(255, 255, 255, 0)');   


context.fillStyle = gradient;
context.fillRect(0, 0, canvas.width, canvas.height);


const texture = new THREE.CanvasTexture(canvas);
texture.needsUpdate = true;


const material = new THREE.MeshBasicMaterial({
map: texture,
transparent: true,
blending: THREE.AdditiveBlending,  
side: THREE.DoubleSide,
depthWrite: false
});


const radius = 45000 / 2; 
const geometry = new THREE.CircleGeometry(radius, 64);


const circle = new THREE.Mesh(geometry, material);


circle.position.set(0, -SPACE_SIZE/2, 0);
circle.rotation.x = -Math.PI / 2; 

return circle;
}



function animate() {
requestAnimationFrame(animate);

const delta = 0.01; 


cameraController.update(delta);


if (!disableAnimations) {
if (!cameraController.isControlActive()) {
    checkHover();
}
updateLabels();
}


try {
if (composer && composer.renderer) {
    composer.render();
} else {
    renderer.render(scene, camera);
}
} catch (error) {
console.error("Render-Fehler:", error);
renderer.render(scene, camera);
}
}


init();