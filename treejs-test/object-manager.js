// object-manager.js
class ObjectManager {
    constructor(scene, camera) {
      this.scene = scene;
      this.camera = camera;
      this.nodes = new Map();
      this.relationships = [];
      this.nodeObjects = new Map();
      this.nodeLabels = new Map();
      this.lineObjects = [];
      this.lineLabels = [];
      this.hoveredObject = null;
      this.selectedObject = null;
      this.labelColorMap = new Map();
      this.nextColorIndex = 0;
      this.infoPanel = document.getElementById('info');
      this.loadingIndicator = document.getElementById('loading');
      this.MIN_DISTANCE = 100;
      this.disableNiceMeshes = false;
      this.groundLightCircle = null;  
      this.useLOD = true;  // Default enabling LOD
      this.frustumCulled = true;  // Enable frustum culling
      this.instancedNodes = null; // For instanced rendering
      this.visibleNodes = new Set(); // Track visible nodes
      this.maxVisibleDistance = 15000; 
      this.isPerformanceModeEnabled = false;
  
      this.neonColors = [
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
  
      this.raycaster = new THREE.Raycaster();
    }
  
  
    setDisableNiceMeshes(value) {
      this.disableNiceMeshes = value;
    }
  
    setUseLOD(value) {
      this.useLOD = value;
      // Update all node meshes if they exist
      if (this.nodeObjects.size > 0) {
        this.updateNodeLODs();
      }
    }
  
    updateNodeLODs() {
      for (const [id, nodeObject] of this.nodeObjects.entries()) {
        const nodeData = this.nodes.get(id);
        if (nodeData) {
          this.updateNodeLOD(nodeObject, nodeData, this.camera.position);
        }
      }
    }
  
    updateNodeLOD(nodeObject, nodeData, cameraPosition, forceQualityCheck = false) {
      if (!this.useLOD || !nodeObject) return;
      
      const distance = cameraPosition.distanceTo(nodeObject.position);
      
      // Base size from connections with a reasonable limit
      const connections = nodeData.connections || 0;
      const baseSize = 20 + Math.min(70, connections * 5);
      
      // Add hysteresis to LOD transitions to prevent flickering
      const farThreshold = 10000;
      const mediumThreshold = 3000;
      
      // Store current LOD before potentially changing it
      const previousLOD = nodeObject.userData.currentLOD;
      const isNewNode = previousLOD === undefined || previousLOD === 'none';
      
      // Get current quality mode
      const isQualityMode = window.currentQualityMode === 'quality';
      
      // Track LOD change for quality reapplication
      let lodChanged = false;
      
      // If currently at far LOD, use different thresholds to prevent oscillation
      if (nodeObject.userData.currentLOD === 'far' && distance > farThreshold * 0.9) {
        // Keep far LOD
      } else if (nodeObject.userData.currentLOD === 'medium' && 
                 distance > mediumThreshold * 0.9 && 
                 distance < farThreshold * 1.1) {
        // Keep medium LOD
      } else {
        // Decide LOD based on distance
        if (distance > farThreshold) {
          if (nodeObject.userData.currentLOD !== 'far') {
            this.applyFarLOD(nodeObject, nodeData, baseSize, isQualityMode);
            nodeObject.userData.currentLOD = 'far';
            lodChanged = true;
          }
        } else if (distance > mediumThreshold) {
          if (nodeObject.userData.currentLOD !== 'medium') {
            this.applyMediumLOD(nodeObject, nodeData, baseSize, isQualityMode);
            nodeObject.userData.currentLOD = 'medium';
            lodChanged = true;
          }
        } else {
          if (nodeObject.userData.currentLOD !== 'close') {
            this.applyCloseLOD(nodeObject, nodeData, baseSize, isQualityMode);
            nodeObject.userData.currentLOD = 'close';
            lodChanged = true;
          }
        }
      }
      
      // Apply quality consistently - when LOD changes OR forced check
      if ((lodChanged || forceQualityCheck || isNewNode) && isQualityMode && !nodeObject.userData.qualityApplied) {
        this.applyNodeQualityEffects(nodeObject, nodeData);
      }
      
      // Handle visibility based on distance
      if (distance > this.maxVisibleDistance * 1.1) {
        if (nodeObject.visible) {
          nodeObject.visible = false;
          if (this.nodeLabels.has(nodeData.id)) {
            this.nodeLabels.get(nodeData.id).visible = false;
          }
        }
      } else if (distance < this.maxVisibleDistance * 0.9 && !nodeObject.visible) {
        nodeObject.visible = true;
        if (this.nodeLabels.has(nodeData.id)) {
          this.nodeLabels.get(nodeData.id).visible = true;
        }
      }
    }
  
    applyFarLOD(nodeObject, nodeData, baseSize, isQualityMode) {
      // Properly clean up existing materials and geometries
      this.cleanupNodeMeshResources(nodeObject);
      
      const primaryLabel = nodeData.labels[0] || `Node ${nodeData.id}`;
      const color = this.getColorForLabel(primaryLabel);
      
      // Use low-poly geometry for far LOD
      nodeObject.geometry = new THREE.OctahedronGeometry(baseSize * 0.8, 0);
      
      // Use passed quality mode parameter
      if (isQualityMode) {
        // Apply high quality material immediately
        nodeObject.material = new THREE.MeshPhysicalMaterial({
          color: color,
          emissive: color,
          emissiveIntensity: 0.3,
          metalness: 0.7,
          roughness: 0.4,
          envMap: window.renderManager ? window.renderManager.envMap : null,
          envMapIntensity: 0.8,
          clearcoat: 0.3,
          clearcoatRoughness: 0.2,
          reflectivity: 0.5
        });
        // Set flag to indicate we've already applied quality
        nodeObject.userData.qualityApplied = true;
      } else {
        // Use standard material
        if (this.disableNiceMeshes) {
          nodeObject.material = new THREE.MeshBasicMaterial({
            color: color,
            wireframe: true
          });
        } else {
          nodeObject.material = new THREE.MeshStandardMaterial({
            color: color,
            emissive: color,
            emissiveIntensity: 0.3,
            flatShading: true,
            roughness: 0.8
          });
        }
      }
    }
  
    
    applyMediumLOD(nodeObject, nodeData, baseSize, isQualityMode) {
      // Clear any children
      while (nodeObject.children.length > 0) {
        const child = nodeObject.children[0];
        if (child.material) child.material.dispose();
        if (child.geometry) child.geometry.dispose();
        nodeObject.remove(child);
      }
      
      if (nodeObject.geometry) nodeObject.geometry.dispose();
      if (nodeObject.material) nodeObject.material.dispose();
      
      const primaryLabel = nodeData.labels[0] || `Node ${nodeData.id}`;
      const color = this.getColorForLabel(primaryLabel);
      
      // Medium detail
      nodeObject.geometry = new THREE.SphereGeometry(baseSize * 0.8, 16, 12);
      
      // Get envMap safely
      const envMap = window.renderManager ? window.renderManager.envMap : null;
      
      // Use passed quality mode parameter - this was missing!
      if (isQualityMode) {
        // Apply high quality material immediately
        nodeObject.material = new THREE.MeshPhysicalMaterial({
          color: color,
          emissive: color,
          emissiveIntensity: 0.3,
          metalness: 0.7,
          roughness: 0.4,
          envMap: envMap,
          envMapIntensity: 0.8,
          clearcoat: 0.4,
          clearcoatRoughness: 0.3,
          reflectivity: 0.6
        });
        
        // Add atmospheric and cloud effects right away in quality mode - this is critical!
        const nodeRadius = nodeObject.geometry.parameters?.radius || 30;
        this.addAtmosphericEffects(nodeObject, color, nodeRadius);
        this.addCloudLayer(nodeObject, color, envMap);
        
        // Set flag to indicate we've already applied quality
        nodeObject.userData.qualityApplied = true;
      } else {
        nodeObject.material = new THREE.MeshStandardMaterial({
          color: color,
          emissive: color,
          emissiveIntensity: 0.5,
          metalness: 0.8,
          roughness: 0.7,
          flatShading: true
        });
        // Reset quality flag when not in quality mode
        nodeObject.userData.qualityApplied = false;
      }
    }
  
    applyCloseLOD(nodeObject, nodeData, baseSize, isQualityMode) {
  // High detail - recreate the original detailed node
  while (nodeObject.children.length > 0) {
    const child = nodeObject.children[0];
    if (child.material) child.material.dispose();
    if (child.geometry) child.geometry.dispose();
    nodeObject.remove(child);
  }
  
  if (nodeObject.geometry) nodeObject.geometry.dispose();
  if (nodeObject.material) nodeObject.material.dispose();
  
  const innerSize = baseSize * 0.8;
  const primaryLabel = nodeData.labels[0] || `Node ${nodeData.id}`;
  const color = this.getColorForLabel(primaryLabel);
  
  // Get envMap safely - ADD THIS LINE
  const envMap = window.renderManager ? window.renderManager.envMap : null;
  
  // Use passed quality mode parameter instead of checking again
  if (isQualityMode) {
    // Full quality detail with outer shell
    const innerGeometry = new THREE.SphereGeometry(innerSize, 32, 32);
    const innerMaterial = new THREE.MeshPhysicalMaterial({
      color: color,
      emissive: color,
      emissiveIntensity: 0.4,
      metalness: 0.8,
      roughness: 0.3,
      envMap: envMap,
      envMapIntensity: 0.9,
      clearcoat: 0.7,
      clearcoatRoughness: 0.2,
      reflectivity: 0.8
    });

    const innerMesh = new THREE.Mesh(innerGeometry, innerMaterial);
    innerMesh.position.set(0, 0, 0);
    
    const outerGeometry = new THREE.SphereGeometry(baseSize, 32, 32);
    const outerMaterial = new THREE.MeshPhysicalMaterial({
      color: color,
      emissive: color,
      emissiveIntensity: 0.3,
      transparent: true,
      opacity: 0.6,
      metalness: 0.7,
      roughness: 0.4,
      envMap: envMap,
      envMapIntensity: 0.7,
      clearcoat: 0.5,
      clearcoatRoughness: 0.3,
      reflectivity: 0.7
    });

    nodeObject.geometry = outerGeometry;
    nodeObject.material = outerMaterial;
    nodeObject.add(innerMesh);
    
    // Set flag to indicate we've already applied quality
    nodeObject.userData.qualityApplied = true;
    
    // Add atmospheric and cloud effects right away in quality mode
    const nodeRadius = nodeObject.geometry.parameters?.radius || 30;
    this.addAtmosphericEffects(nodeObject, color, nodeRadius);
    this.addCloudLayer(nodeObject, color, envMap);
    
  } else if (this.disableNiceMeshes) {
    // Simple high detail version
    nodeObject.geometry = new THREE.SphereGeometry(baseSize * 0.8, 32, 24);
    nodeObject.material = new THREE.MeshStandardMaterial({
      color: color,
      emissive: color,
      emissiveIntensity: 0.5,
      metalness: 0.9,
      roughness: 0.7
    });
    // Reset quality flag when not in quality mode
    nodeObject.userData.qualityApplied = false;
  } else {
    // Full detail with outer shell
    const innerGeometry = new THREE.SphereGeometry(innerSize, 32, 32);
    const innerMaterial = new THREE.MeshStandardMaterial({
      color: color,
      emissive: color,
      emissiveIntensity: 0.5,
      metalness: 0.9,
      roughness: 0.7
    });

    const innerMesh = new THREE.Mesh(innerGeometry, innerMaterial);
    innerMesh.position.set(0, 0, 0);
    
    const outerGeometry = new THREE.SphereGeometry(baseSize, 32, 32);
    const outerMaterial = new THREE.MeshStandardMaterial({
      color: color,
      emissive: color,
      emissiveIntensity: 0.5,
      transparent: true,
      opacity: 0.5,
      metalness: 0.9,
      roughness: 0.7
    });

    nodeObject.geometry = outerGeometry;
    nodeObject.material = outerMaterial;
    nodeObject.add(innerMesh);
    
    // Reset quality flag when not in quality mode
    nodeObject.userData.qualityApplied = false;
  }
}
  
    createTextLabel(text, color = 0xffffff) {
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
  
    getColorForLabel(label) {
      if (this.labelColorMap.has(label)) {
        return this.labelColorMap.get(label);
      }
      
      let color;
      
      if (this.nextColorIndex < this.neonColors.length) {
        color = this.neonColors[this.nextColorIndex];
        this.nextColorIndex++;
      } else {
        const baseColor = this.neonColors[this.nextColorIndex % this.neonColors.length];
        
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
        
        this.nextColorIndex++;
      }
      
      this.labelColorMap.set(label, color);
      
      return color;
    }
  
    createNode(id, nodeData) {
      const size = 20 + Math.min(70, nodeData.connections * 5);
      
      // Create a simple placeholder mesh first
      const geometry = new THREE.SphereGeometry(size * 0.8, 8, 6);
      const primaryLabel = nodeData.labels[0] || `Node ${id}`;
      const color = this.getColorForLabel(primaryLabel);
      
      // Check quality mode BEFORE creating initial material
      const isQualityMode = window.renderManager && window.renderManager.currentQualityMode === 'quality';
      
      // Use different initial material based on quality mode
      const material = isQualityMode ? 
        new THREE.MeshPhysicalMaterial({
          color: color,
          emissive: color,
          emissiveIntensity: 0.3,
          metalness: 0.7,
          roughness: 0.4,
          envMap: window.renderManager ? window.renderManager.envMap : null,
          envMapIntensity: 0.8,
          clearcoat: 0.6,
          clearcoatRoughness: 0.2,
          reflectivity: 0.8
        }) :
        new THREE.MeshStandardMaterial({
          color: color,
          emissive: color,
          emissiveIntensity: 0.3
        });
      
      const nodeMesh = new THREE.Mesh(geometry, material);
      nodeMesh.position.set(
        nodeData.position.x,
        nodeData.position.y, 
        nodeData.position.z
      );
      
      nodeMesh.userData = {
        id: id,
        labels: nodeData.labels,
        properties: nodeData.properties,
        type: 'node',
        currentLOD: 'none',
        qualityApplied: isQualityMode // Track quality application from the start
      };
      
      nodeMesh.frustumCulled = this.frustumCulled;
      
      this.scene.add(nodeMesh);
      this.nodeObjects.set(id, nodeMesh);
      
      // Now update LOD after basic setup is complete
      this.updateNodeLOD(nodeMesh, nodeData, this.camera.position, true); // Force quality check
      
      // If in quality mode, explicitly apply quality effects now
      if (isQualityMode) {
        this.applyNodeQualityEffects(nodeMesh, nodeData);
      }
      
      const labelText = primaryLabel;
      const labelSprite = this.createTextLabel(labelText, color);
      labelSprite.position.set(
        nodeData.position.x,
        nodeData.position.y + size + 30,
        nodeData.position.z
      );
      
      labelSprite.frustumCulled = this.frustumCulled;
      this.scene.add(labelSprite);
      this.nodeLabels.set(id, labelSprite);
    }
  
    createRelationship(relationship) {
      const startNode = this.nodeObjects.get(relationship.startId);
      const endNode = this.nodeObjects.get(relationship.endId);
      
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
      
      // Enable frustum culling
      line.frustumCulled = this.frustumCulled;
      
      this.scene.add(line);
      this.lineObjects.push(line);
      
      const labelText = relationship.label || `Rel ${relationship.id}`;
      const labelSprite = this.createTextLabel(labelText, 0xffff00); 
      
      const midPoint = new THREE.Vector3().addVectors(
        startNode.position,
        endNode.position
      ).multiplyScalar(0.5);
      
      midPoint.y += 20;
      
      labelSprite.position.copy(midPoint);
      labelSprite.frustumCulled = this.frustumCulled;
      
      this.scene.add(labelSprite);
      this.lineLabels.push({
        sprite: labelSprite,
        startId: relationship.startId,
        endId: relationship.endId
      });
    }
  
    addGroundLightCircle(space_size) {
        if (isNaN(space_size)) {
            return;
        }

      const canvas = document.createElement('canvas');
      canvas.width = 1024;
      canvas.height = 1024;
      const context = canvas.getContext('2d');
  
      const gradient = context.createRadialGradient(
        canvas.width / 2, 
        canvas.height / 2, 
        0, 
        canvas.width / 2, 
        canvas.height / 2, 
        canvas.width / 2 
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
  
      const radius = (space_size-5000) / 2; 
      const geometry = new THREE.CircleGeometry(radius, 64);
  
      const circle = new THREE.Mesh(geometry, material);
  
      circle.position.set(0, -space_size/2, 0);
      circle.rotation.x = -Math.PI / 2; 
  
      this.groundLightCircle = circle;  
      return circle;
    }
  
    updateGroundLightCircle(space_size) {
      if (this.groundLightCircle && !isNaN(space_size)) {
        this.groundLightCircle.position.set(0, -space_size/2, 0);
        
    
        const radius = (space_size-5000) / 2;
        this.groundLightCircle.geometry.dispose(); 
        this.groundLightCircle.geometry = new THREE.CircleGeometry(radius, 64);
      }
    }
  
    updateInfoPanel(data) {
      // Create a container with improved styling
      let html = `<div style="font-family: 'Segoe UI', Arial, sans-serif; color: #e0e0e0;">`;
      
      // Add a header with background color based on data type
      const headerColor = data.type === 'node' ? '#3498db' : '#e74c3c';
      const headerIcon = data.type === 'node' ? '●' : '↔';
      
      html += `
        <div style="background-color: ${headerColor}; padding: 8px 12px; border-radius: 4px 4px 0 0; 
                    margin-bottom: 10px; display: flex; align-items: center;">
          <span style="font-size: 18px; margin-right: 8px;">${headerIcon}</span>
          <h3 style="margin: 0; font-weight: bold; text-transform: capitalize;">${data.type}</h3>
        </div>`;
      
      if (data.type === 'node') {
        // Node information with improved layout
        const labelList = data.labels.join(', ');
        
        // Get connections count from the nodes Map using the node's ID
        let connectionsCount = 0;
        if (this.nodes.has(data.id)) {
          connectionsCount = this.nodes.get(data.id).connections || 0;
        }
        
        html += `
          <div style="margin-bottom: 12px; background-color: rgba(52, 152, 219, 0.1); border-left: 3px solid #3498db; padding: 8px;">
            <div style="margin-bottom: 6px;"><strong>Labels:</strong> ${labelList}</div>
            <div style="margin-bottom: 6px;"><strong>ID:</strong> ${data.id}</div>
            <div><strong>Relationships:</strong> ${connectionsCount}</div>
          </div>`;
        
        // Properties section with improved styling
        if (data.properties && Object.keys(data.properties).length > 0) {
          html += `<div style="background-color: rgba(46, 204, 113, 0.1); border-left: 3px solid #2ecc71; padding: 8px;">
                    <h4 style="margin-top: 0; margin-bottom: 8px; color: #2ecc71;">Properties</h4>
                    <table style="width: 100%; border-collapse: collapse;">`;
                    
          for (const [key, value] of Object.entries(data.properties)) {
            html += `<tr>
                      <td style="padding: 3px; border-bottom: 1px solid rgba(255,255,255,0.1);"><strong>${key}</strong></td>
                      <td style="padding: 3px; border-bottom: 1px solid rgba(255,255,255,0.1);">${value}</td>
                    </tr>`;
          }
          
          html += `</table></div>`;
        }
      } else if (data.type === 'relationship') {
        // Relationship information with improved layout
        html += `
          <div style="margin-bottom: 12px; background-color: rgba(231, 76, 60, 0.1); border-left: 3px solid #e74c3c; padding: 8px;">
            <div style="margin-bottom: 6px;"><strong>Type:</strong> ${data.label || 'Undefined'}</div>
            <div style="margin-bottom: 6px;"><strong>ID:</strong> ${data.id}</div>
          </div>
          
          <div style="display: flex; margin-bottom: 10px;">
            <div style="flex: 1; background-color: rgba(52, 152, 219, 0.1); border-left: 3px solid #3498db; padding: 8px; cursor: pointer;"
                 onclick="objectManager.focusOnNode('${data.startId}')" 
                 onmouseover="this.style.backgroundColor='rgba(52, 152, 219, 0.3)'"
                 onmouseout="this.style.backgroundColor='rgba(52, 152, 219, 0.1)'">
              <h4 style="margin-top: 0; margin-bottom: 8px; color: #3498db;">From</h4>
              <div><strong>ID:</strong> ${data.startId}</div>
            </div>
            <div style="width: 20px; display: flex; justify-content: center; align-items: center;">→</div>
            <div style="flex: 1; background-color: rgba(52, 152, 219, 0.1); border-left: 3px solid #3498db; padding: 8px; cursor: pointer;"
                 onclick="objectManager.focusOnNode('${data.endId}')"
                 onmouseover="this.style.backgroundColor='rgba(52, 152, 219, 0.3)'"
                 onmouseout="this.style.backgroundColor='rgba(52, 152, 219, 0.1)'">
              <h4 style="margin-top: 0; margin-bottom: 8px; color: #3498db;">To</h4>
              <div><strong>ID:</strong> ${data.endId}</div>
            </div>
          </div>`;
      }
      
      // Close the container div
      html += `</div>`;
      
      // Add the close button with improved styling
      const closeButton = `
        <button class="btn" 
          style="position: absolute; top: 8px; right: 8px; padding: 4px 8px; 
                font-size: 12px; background-color: rgba(0,0,0,0.4); color: #fff; 
                border: none; border-radius: 3px; cursor: pointer; transition: background-color 0.2s;"
          onmouseover="this.style.backgroundColor='rgba(255,0,0,0.4)'"
          onmouseout="this.style.backgroundColor='rgba(0,0,0,0.4)'"
          onclick="clearSelection()">✕</button>`;
      
      html += closeButton;
      
      this.infoPanel.innerHTML = html;
      
      // Vertically center the panel
      this.infoPanel.style.top = "50%";
      this.infoPanel.style.transform = "translateY(-50%)";
    }
  
    clearSelection() {
      if (this.selectedObject) {
        this.resetObjectHighlight(this.selectedObject);
        this.selectedObject = null;
      }
      this.infoPanel.style.display = 'none';
    }
  
    checkHover() {

        if (this.selectedObject) return;
        
        const mouse = cameraController.getMouseCoordinates();
        
        raycaster.setFromCamera(mouse, camera);
        
        
        const objects = [...this.nodeObjects.values(), ...this.lineObjects];
        const intersects = raycaster.intersectObjects(objects);
        
        if (intersects.length > 0) {
        const object = intersects[0].object;
        
        if (this.hoveredObject !== object) {
          
            if (this.hoveredObject) {
                if (this.hoveredObject.userData.type === 'node') {
                    this.hoveredObject.material.emissiveIntensity = 0.7;
                    this.hoveredObject.scale.set(1, 1, 1);
                } else {
                    this.hoveredObject.material.opacity = 0.6;
                }
            }
        
            if (object.userData.type === 'node') {
                object.material.emissiveIntensity = 1.5;
                object.scale.set(1.1, 1.1, 1.1);
            } else {
                object.material.opacity = 1.0;
            }
            
            this.updateInfoPanel(object.userData);
            
            this.hoveredObject = object;
        }
        
        this.infoPanel.style.display = 'block';
        } else {
        
        if (this.hoveredObject) {
            if (this.hoveredObject.userData.type === 'node') {
                this.hoveredObject.material.emissiveIntensity = 0.7;
                this.hoveredObject.scale.set(1, 1, 1);
            } else {
                this.hoveredObject.material.opacity = 0.6;
            }
            
            this.hoveredObject = null;
            this.infoPanel.style.display = 'none';
        }
        }
        }
  
    handleClick(mouseCoordinates) {
      this.raycaster.setFromCamera(mouseCoordinates, this.camera);
  
      const objects = [...this.nodeObjects.values(), ...this.lineObjects];
      const intersects = this.raycaster.intersectObjects(objects);
  
      if (intersects.length > 0) {
        const object = intersects[0].object;
        
        // First reset the previous selection
        if (this.selectedObject && this.selectedObject !== object) {
          this.resetObjectHighlight(this.selectedObject);
        }
  
        this.selectedObject = object;
  
        if (object.userData.type === 'node') {
          this.highlightNode(object);
        } else {
          this.highlightRelationship(object);
        }
  
        // Reset any hover that isn't the selected object
        if (this.hoveredObject && this.hoveredObject !== this.selectedObject) {
          this.resetObjectHighlight(this.hoveredObject);
          this.hoveredObject = null;
        }
  
        this.updateInfoPanel(object.userData);
        this.infoPanel.style.display = 'block';
      } else {
        if (this.selectedObject) {
          this.resetObjectHighlight(this.selectedObject);
          this.selectedObject = null;
        }
        this.infoPanel.style.display = 'none';
      }
    }
  
    highlightNode(node) {
      if (!node || !node.material) return;
      
      const originalColor = node.userData.originalColor || node.material.color.getHex();
      node.userData.originalColor = originalColor;
      
      // Only store emissive properties if the material supports it
      if (node.material.emissive) {
        node.userData.originalEmissive = node.material.emissive.getHex();
        node.userData.originalEmissiveIntensity = node.material.emissiveIntensity || 0.5;
      }
      
      // Store original materials if we have children (for complex nodes)
      if (node.children && node.children.length > 0) {
        node.userData.childrenOriginalMaterials = [];
        node.children.forEach((child, index) => {
          if (child.material) {
            node.userData.childrenOriginalMaterials[index] = {
              color: child.material.color ? child.material.color.getHex() : 0xffffff
            };
            
            // Only store emissive properties if the material supports it
            if (child.material.emissive) {
              node.userData.childrenOriginalMaterials[index].emissive = 
                child.material.emissive.getHex();
              node.userData.childrenOriginalMaterials[index].emissiveIntensity = 
                child.material.emissiveIntensity || 0.5;
            }
            
            // Apply highlight effect to child material
            if (child.material.emissive) {
              child.material.emissive.setHex(0xffffff);
              child.material.emissiveIntensity = 1.5;
            } else {
              // For materials without emissive, modify color instead
              child.material.color.setHex(0xffffff);
            }
          }
        });
      }
      
      // Apply highlight effect to main material
      if (node.material.emissive) {
        node.material.emissive.setHex(0xffffff);
        node.material.emissiveIntensity = 1.5;
      } else {
        // For materials without emissive, modify color instead
        node.material.color.setHex(0xffffff);
      }
    }
  
    highlightRelationship(line) {
      if (!line || !line.material) return;
      
      line.userData.originalColor = line.material.color.getHex();
      line.userData.originalOpacity = line.material.opacity;
      
      line.material.color.setHex(0xffffff);
      line.material.opacity = 0.8;
    }
  
    resetObjectHighlight(object) {
      if (!object || !object.material) return;
      
      if (object.userData.type === 'node') {
        // Restore emissive properties only if the material supports it
        if (object.material.emissive && object.userData.originalEmissive !== undefined) {
          object.material.emissive.setHex(object.userData.originalEmissive);
        }
        if (object.userData.originalEmissiveIntensity !== undefined && object.material.emissive) {
          object.material.emissiveIntensity = object.userData.originalEmissiveIntensity;
        }
        
        // Always restore original color
        if (object.userData.originalColor !== undefined) {
          object.material.color.setHex(object.userData.originalColor);
        }
        
        // Restore children materials if any
        if (object.userData.childrenOriginalMaterials && object.children) {
          object.children.forEach((child, index) => {
            const originalMaterial = object.userData.childrenOriginalMaterials[index];
            if (child.material && originalMaterial) {
              // Restore color
              if (originalMaterial.color !== undefined) {
                child.material.color.setHex(originalMaterial.color);
              }
              
              // Restore emissive properties only if material supports it
              if (child.material.emissive && originalMaterial.emissive !== undefined) {
                child.material.emissive.setHex(originalMaterial.emissive);
              }
              if (originalMaterial.emissiveIntensity !== undefined && child.material.emissive) {
                child.material.emissiveIntensity = originalMaterial.emissiveIntensity;
              }
            }
          });
        }
      } else if (object.userData.type === 'relationship') {
        if (object.userData.originalColor !== undefined) {
          object.material.color.setHex(object.userData.originalColor);
        }
        if (object.userData.originalOpacity !== undefined) {
          object.material.opacity = object.userData.originalOpacity;
        }
      }
    }
  
    updateLabels() {
      // Update LODs if enabled
      if (this.useLOD) {
        for (const [nodeId, nodeObject] of this.nodeObjects.entries()) {
          const nodeData = this.nodes.get(nodeId);
          if (nodeData) {
            this.updateNodeLOD(nodeObject, nodeData, this.camera.position);
          }
        }
      }
    
      // Original label update code
      for (const [nodeId, label] of this.nodeLabels.entries()) {
        const node = this.nodeObjects.get(nodeId);
        if (node && node.visible) {
          label.lookAt(this.camera.position);
          
          const distance = this.camera.position.distanceTo(node.position);
          
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
          
         
          label.visible = distance <= this.maxVisibleDistance;
        } else if (node && !node.visible) {
          label.visible = false;
        }
      }
      
      // Relationship labels update
      for (const labelInfo of this.lineLabels) {
        const startNode = this.nodeObjects.get(labelInfo.startId);
        const endNode = this.nodeObjects.get(labelInfo.endId);
    
        if (startNode && endNode && startNode.visible && endNode.visible) {
          const midPoint = new THREE.Vector3().addVectors(
            startNode.position,
            endNode.position
          ).multiplyScalar(0.5);
          
          midPoint.y += 20;
          
          labelInfo.sprite.position.copy(midPoint);
          labelInfo.sprite.lookAt(this.camera.position);
          
          const distance = this.camera.position.distanceTo(midPoint);
          
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
          labelInfo.sprite.visible = distance <= this.maxVisibleDistance;
        } else {
          labelInfo.sprite.visible = false;
        }
      }
    }
  
    setupInstancedRendering() {
      
      if (this.instancedNodes) {
        this.scene.remove(this.instancedNodes);
        this.instancedNodes.geometry.dispose();
        this.instancedNodes.material.dispose();
        this.instancedNodes = null;
      }
      
     
      if (this.nodeObjects.size === 0 || !this.disableNiceMeshes) {
        return;
      }
      
      const nodeCount = this.nodeObjects.size;
      
      // Create instanced mesh with a MeshStandardMaterial instead of MeshBasicMaterial
      const geometry = new THREE.SphereGeometry(30, 8, 6);
      const material = new THREE.MeshStandardMaterial({
        metalness: 0.3,
        roughness: 0.7
      });
      
      const instancedMesh = new THREE.InstancedMesh(
        geometry,
        material,
        nodeCount
      );
      
      let index = 0;
      const tempMatrix = new THREE.Matrix4();
      const tempColor = new THREE.Color();
      
      // Set position and color for each instance
      for (const [nodeId, nodeObject] of this.nodeObjects.entries()) {
        const nodeData = this.nodes.get(nodeId);
        if (nodeData) {
          const primaryLabel = nodeData.labels[0] || `Node ${nodeId}`;
          const color = this.getColorForLabel(primaryLabel);
          
          tempMatrix.setPosition(
            nodeData.position.x,
            nodeData.position.y,
            nodeData.position.z
          );
          
          tempColor.set(color);
          
          instancedMesh.setMatrixAt(index, tempMatrix);
          instancedMesh.setColorAt(index, tempColor);
          
          index++;
        }
      }
      
      instancedMesh.instanceMatrix.needsUpdate = true;
      if (instancedMesh.instanceColor) {
        instancedMesh.instanceColor.needsUpdate = true;
      }
      
      this.instancedNodes = instancedMesh;
      this.scene.add(instancedMesh);
    }
  
    setPerformanceMode(enabled) {
        console.log('setPerformanceMode', enabled);
      if (enabled & !this.isPerformanceModeEnabled) {
        
        this.setupInstancedRendering();
        
     
        for (const nodeObject of this.nodeObjects.values()) {
          nodeObject.visible = false;
        }
        
      
        if (this.instancedNodes) {
          this.instancedNodes.visible = true;
        }
        
        this.maxVisibleDistance = 15000;

        this.isPerformanceModeEnabled = true;
      } else if (!enabled && this.isPerformanceModeEnabled) {
      
        if (this.instancedNodes) {
          this.instancedNodes.visible = false;
        }
        
        for (const nodeObject of this.nodeObjects.values()) {
          nodeObject.visible = true;
        }

        this.maxVisibleDistance = 50000;
        this.isPerformanceModeEnabled = false;
      }
    }
  
    createVisualization(nodes, relationships) {
      this.nodes = nodes;
      this.relationships = relationships;
      
      const batchSize = 50;
      let nodeCount = nodes.size;
      let processedCount = 0;
      const loadingIndicator = this.loadingIndicator;
      
      const createNodesInBatches = () => {
        const nodesToProcess = Array.from(nodes.entries())
          .slice(processedCount, processedCount + batchSize);
        
        nodesToProcess.forEach(([id, nodeData]) => {
          this.createNode(id, nodeData);
          processedCount++;
        });
        
        loadingIndicator.textContent = `Lade Nodes: ${processedCount}/${nodeCount}`;
        
        if (processedCount < nodeCount) {
          setTimeout(createNodesInBatches, 10);
        } else {
          createRelationshipsInBatches();
        }
      };
    
      const createRelationshipsInBatches = () => {
        const totalRelationships = relationships.length;
        let processedRelationships = 0;
        
        const processBatch = () => {
          const batchSize = 50;
          const relToProcess = relationships.slice(
            processedRelationships, 
            processedRelationships + batchSize
          );
          
          relToProcess.forEach(rel => {
            this.createRelationship(rel);
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
        };
        
        processBatch();
      };
      
      createNodesInBatches();
    }

    setDynamicRendering(enabled) {
      this.dynamicRendering = enabled;
      
      if (enabled) {
        this.updateDynamicNodeVisibility();
      } else {
       
        for (const [nodeId, nodeObject] of this.nodeObjects.entries()) {
          nodeObject.visible = true;
          const label = this.nodeLabels.get(nodeId);
          if (label) label.visible = true;
        }
      }
    }
    
    updateDynamicNodeVisibility() {
      if (!this.dynamicRendering) return;
      
    
      if (!this._frustum) {
        this._frustum = new THREE.Frustum();
        this._projScreenMatrix = new THREE.Matrix4();
      }
      
      this._projScreenMatrix.multiplyMatrices(
        this.camera.projectionMatrix,
        this.camera.matrixWorldInverse
      );
      this._frustum.setFromProjectionMatrix(this._projScreenMatrix);
      
     
      const visibleNodes = new Set();
      const nodesToUpdate = [];
      const labelsToUpdate = [];
      
     
      for (const [nodeId, nodeObject] of this.nodeObjects.entries()) {
        const isInFrustum = this._frustum.containsPoint(nodeObject.position);
        const distance = this.camera.position.distanceTo(nodeObject.position);
        const isVisible = isInFrustum && distance <= this.maxVisibleDistance;
        
        if (isVisible !== nodeObject.visible) {
          nodesToUpdate.push({ node: nodeObject, visible: isVisible, nodeId: nodeId });
        }
        
        if (isVisible) {
          visibleNodes.add(nodeId);
        }
      }
      
    
      for (const update of nodesToUpdate) {
        update.node.visible = update.visible;
        
        // Apply quality effects when nodes become visible
        if (update.visible && 
            window.renderManager && 
            window.renderManager.currentQualityMode === 'quality' &&
            !update.node.userData.qualityApplied) {
          const nodeData = this.nodes.get(update.nodeId);
          if (nodeData) {
            this.applyNodeQualityEffects(update.node, nodeData);
          }
        }
      }
      
     
      for (const [nodeId, label] of this.nodeLabels.entries()) {
        const isVisible = visibleNodes.has(nodeId);
        if (label.visible !== isVisible) {
          labelsToUpdate.push({ label, visible: isVisible });
        }
      }
      
      for (const update of labelsToUpdate) {
        update.label.visible = update.visible;
      }
      

      this.updateRelationshipVisibility(visibleNodes);
    }
    
    updateRelationshipVisibility(visibleNodes) {
      const linesToUpdate = [];
      
      for (let i = 0; i < this.lineObjects.length; i++) {
        const line = this.lineObjects[i];
        const startNodeId = line.userData.startId;
        const endNodeId = line.userData.endId;
        
        const eitherNodeVisible = visibleNodes.has(startNodeId) || visibleNodes.has(endNodeId);
        
        if (line.visible !== eitherNodeVisible) {
          linesToUpdate.push({ line, visible: eitherNodeVisible, index: i });
        }
      }
      
   
      for (const update of linesToUpdate) {
        update.line.visible = update.visible;
        
        if (update.index < this.lineLabels.length) {
          this.lineLabels[update.index].sprite.visible = update.visible;
        }
      }
    }

   
    

    getPerformanceIsEnabled() {
      return this.isPerformanceModeEnabled;
    }

    applyQualityMaterials(envMap) {
      // Force setting currentQualityMode to 'quality'
      if (window.renderManager) {
        window.renderManager.currentQualityMode = 'quality';
      }
      
      for (const [nodeId, nodeObject] of this.nodeObjects.entries()) {
        const nodeData = this.nodes.get(nodeId);
        if (!nodeData) continue;
        
        // Reset quality applied flag to ensure proper update
        nodeObject.userData.qualityApplied = false;
        
        // Force material update
        const primaryLabel = nodeData.labels[0] || `Node ${nodeId}`;
        const color = this.getColorForLabel(primaryLabel);
        
        // Remove existing atmospheric layers and cloud mesh
        if (nodeObject.userData.atmosphereLayers) {
          for (const layer of nodeObject.userData.atmosphereLayers) {
            if (layer && layer.parent === nodeObject) {
              if (layer.material) layer.material.dispose();
              if (layer.geometry) layer.geometry.dispose();
              nodeObject.remove(layer);
            }
          }
          nodeObject.userData.atmosphereLayers = [];
        }
        
        if (nodeObject.userData.cloudMesh) {
          nodeObject.remove(nodeObject.userData.cloudMesh);
          if (nodeObject.userData.cloudMesh.material) {
            nodeObject.userData.cloudMesh.material.dispose();
          }
          if (nodeObject.userData.cloudMesh.geometry) {
            nodeObject.userData.cloudMesh.geometry.dispose();
          }
          nodeObject.userData.cloudMesh = null;
        }
        
        // Clean up existing material
        if (nodeObject.material) {
          nodeObject.material.dispose();
        }
        
        // Determine node radius based on current geometry
        const nodeRadius = nodeObject.geometry.parameters?.radius || 30;
        
        // Create planetary surface material with enhanced features based on LOD
        if (nodeObject.userData.currentLOD === 'far') {
          nodeObject.material = new THREE.MeshPhysicalMaterial({
            color: color,
            emissive: color,
            emissiveIntensity: 0.3,
            metalness: 0.7,
            roughness: 0.4,
            envMap: envMap,
            envMapIntensity: 0.8,
            clearcoat: 0.3,
            clearcoatRoughness: 0.2,
            reflectivity: 0.5
          });
          this.addAtmosphericEffects(nodeObject, color, nodeRadius);
          this.addCloudLayer(nodeObject, color, envMap);
        }
        else if (nodeObject.userData.currentLOD === 'medium') {
          nodeObject.material = new THREE.MeshPhysicalMaterial({
            color: color,
            emissive: color,
            emissiveIntensity: 0.3,
            metalness: 0.7,
            roughness: 0.4,
            envMap: envMap,
            envMapIntensity: 0.8,
            clearcoat: 0.4,
            clearcoatRoughness: 0.3,
            reflectivity: 0.6
          });
          this.addAtmosphericEffects(nodeObject, color, nodeRadius);
          this.addCloudLayer(nodeObject, color, envMap);
        }
        else { // close or default
          // For close LOD, create a more complex object with inner and outer shells
          
          // First, clear any existing children
          while (nodeObject.children.length > 0) {
            const child = nodeObject.children[0];
            if (child.material) child.material.dispose();
            if (child.geometry) child.geometry.dispose();
            nodeObject.remove(child);
          }
          
          if (nodeObject.geometry) nodeObject.geometry.dispose();
          
          const innerSize = nodeRadius * 0.8;
          const outerSize = nodeRadius;
          
          const innerGeometry = new THREE.SphereGeometry(innerSize, 32, 32);
          const innerMaterial = new THREE.MeshPhysicalMaterial({
            color: color,
            emissive: color,
            emissiveIntensity: 0.4,
            metalness: 0.8,
            roughness: 0.3,
            envMap: envMap,
            envMapIntensity: 0.9,
            clearcoat: 0.7,
            clearcoatRoughness: 0.2,
            reflectivity: 0.8
          });
  
          const innerMesh = new THREE.Mesh(innerGeometry, innerMaterial);
          innerMesh.position.set(0, 0, 0);
          
          const outerGeometry = new THREE.SphereGeometry(outerSize, 32, 32);
          const outerMaterial = new THREE.MeshPhysicalMaterial({
            color: color,
            emissive: color,
            emissiveIntensity: 0.3,
            transparent: true,
            opacity: 0.6,
            metalness: 0.7,
            roughness: 0.4,
            envMap: envMap,
            envMapIntensity: 0.7,
            clearcoat: 0.5,
            clearcoatRoughness: 0.3,
            reflectivity: 0.7
          });
  
          nodeObject.geometry = outerGeometry;
          nodeObject.material = outerMaterial;
          nodeObject.add(innerMesh);
          
          // Add atmospheric and cloud effects
          this.addAtmosphericEffects(nodeObject, color, nodeRadius);
          this.addCloudLayer(nodeObject, color, envMap);
        }
        
        // Mark as quality applied
        nodeObject.userData.qualityApplied = true;
      }
      
      // Initialize cloud texture cache if needed
      if (!this._cloudTextureCache) {
        this._cloudTextureCache = this.generateCloudTexture();
      }
    }
    
    applyStandardMaterials() {
      // Force setting currentQualityMode to 'standard'
      if (window.renderManager) {
        window.renderManager.currentQualityMode = 'standard';
      }
      
      for (const [nodeId, nodeObject] of this.nodeObjects.entries()) {
        const nodeData = this.nodes.get(nodeId);
        if (!nodeData) continue;
        
        // Reset the quality applied flag
        nodeObject.userData.qualityApplied = false;
        
        // Remove quality-specific effects
        if (nodeObject.userData.glowMesh) {
          nodeObject.userData.glowMesh.visible = false;
        }
        
        // Remove atmospheric effects and cloud mesh
        if (nodeObject.userData.atmosphereLayers) {
          for (const layer of nodeObject.userData.atmosphereLayers) {
            if (layer && layer.parent === nodeObject) {
              if (layer.material) layer.material.dispose();
              if (layer.geometry) layer.geometry.dispose();
              nodeObject.remove(layer);
            }
          }
          nodeObject.userData.atmosphereLayers = [];
        }
        
        if (nodeObject.userData.cloudMesh) {
          nodeObject.remove(nodeObject.userData.cloudMesh);
          if (nodeObject.userData.cloudMesh.material) {
            nodeObject.userData.cloudMesh.material.dispose();
          }
          if (nodeObject.userData.cloudMesh.geometry) {
            nodeObject.userData.cloudMesh.geometry.dispose();
          }
          nodeObject.userData.cloudMesh = null;
        }
        
        // Now update node LOD with standard materials
        this.updateNodeLOD(nodeObject, nodeData, this.camera.position);
      }
    }
    
    applyPerformanceMaterials() {
      for (const [nodeId, nodeObject] of this.nodeObjects.entries()) {
      
        if (nodeObject.userData.glowMesh) {
          nodeObject.userData.glowMesh.visible = false;
        }
        
      
      }
    }
    
    enableEdgeGlow(enabled) {
      this.edgeGlowEnabled = enabled;
      
      for (const line of this.lineObjects) {
        if (enabled) {
          if (!line.userData.glowLine) {
          
            const startNode = this.nodeObjects.get(line.userData.startId);
            const endNode = this.nodeObjects.get(line.userData.endId);
            
            if (!startNode || !endNode) continue;
            
         
            const points = line.geometry.attributes.position;
            const glowGeometry = new THREE.BufferGeometry().setFromPoints(
              Array(points.count).fill().map((_, i) => 
                new THREE.Vector3(
                  points.getX(i), 
                  points.getY(i), 
                  points.getZ(i)
                )
              )
            );
            
      
            const relationshipColor = 0x00ffff;
            
            const glowMaterial = new THREE.LineBasicMaterial({
              color: relationshipColor,
              transparent: true,
              opacity: 0.3,
              linewidth: 3
            });
            
            const glowLine = new THREE.Line(glowGeometry, glowMaterial);
            this.scene.add(glowLine);
            
            line.userData.glowLine = glowLine;
          } else {
            line.userData.glowLine.visible = true;
          }
        } else if (line.userData.glowLine) {
          line.userData.glowLine.visible = false;
        }
      }
    }
    
    focusOnNode(nodeId) {
      const nodeObject = this.nodeObjects.get(nodeId);
      if (nodeObject) {
        this.focusOnObject(nodeObject);
      }
    }
    
   
    focusOnObject(object) {
        if (!object) return;
        

        const targetPosition = object.position.clone();
        
      
        const offset = new THREE.Vector3(0, 20, 100);
        const cameraTargetPosition = targetPosition.clone().add(offset);
        
       
        if (window.cameraController) {
            window.cameraController.moveTo(cameraTargetPosition, targetPosition, 1000); 
        }
    }
    
    // Add this method to handle keypress navigation
    handleKeyNavigation(key) {
       
        if (key === 'f' || key === 'F') {
            if (this.selectedObject) {
                this.focusOnObject(this.selectedObject);
            }
        } else if (key === 'from' || key === 'arrowleft') {
            if (this.selectedObject && this.selectedObject.userData.type === 'relationship') {
                const startNode = this.nodeObjects.get(this.selectedObject.userData.startId);
                if (startNode) {
                    this.focusOnObject(startNode);
                }
            }
        } else if (key === 'to' || key === 'arrowright') {
            if (this.selectedObject && this.selectedObject.userData.type === 'relationship') {
                const endNode = this.nodeObjects.get(this.selectedObject.userData.endId);
                if (endNode) {
                    this.focusOnObject(endNode);
                }
            }
        }
    }

    cleanupNodeMeshResources(nodeObject) {
   
      while (nodeObject.children.length > 0) {
        const child = nodeObject.children[0];
        if (child.material) {
          if (Array.isArray(child.material)) {
            child.material.forEach(mat => {
              if (mat && mat.dispose) mat.dispose();
            });
          } else if (child.material.dispose) {
            child.material.dispose();
          }
        }
        if (child.geometry && child.geometry.dispose) child.geometry.dispose();
        nodeObject.remove(child);
      }
      
    
      if (nodeObject.geometry && nodeObject.geometry.dispose) nodeObject.geometry.dispose();
      if (nodeObject.material) {
        if (Array.isArray(nodeObject.material)) {
          nodeObject.material.forEach(mat => {
            if (mat && mat.dispose) mat.dispose();
          });
        } else if (nodeObject.material.dispose) {
          nodeObject.material.dispose();
        }
      }
    }

    applyNodeQualityEffects(nodeObject, nodeData) {
      // Get color for the node
      const primaryLabel = nodeData.labels[0] || `Node ${nodeData.id}`;
      const color = this.getColorForLabel(primaryLabel);
      
      // Mark this node as having quality applied
      nodeObject.userData.qualityApplied = true;
      
      // Enhance the existing material
      if (nodeObject.material) {
        if (window.renderManager && window.renderManager.envMap) {
          nodeObject.material.envMap = window.renderManager.envMap;
          nodeObject.material.envMapIntensity = 0.8;
          nodeObject.material.needsUpdate = true;
        }
    
        if (nodeObject.material.type === 'MeshStandardMaterial' || 
            nodeObject.material.type === 'MeshPhysicalMaterial') {
          nodeObject.material.metalness = 0.7;
          nodeObject.material.roughness = 0.4;
          nodeObject.material.emissive = new THREE.Color(color);
          nodeObject.material.emissiveIntensity = 0.3;
          
       
          if (nodeObject.material.type !== 'MeshPhysicalMaterial') {
            const oldMaterial = nodeObject.material;
            const newMaterial = new THREE.MeshPhysicalMaterial({
              color: oldMaterial.color.clone(),
              emissive: oldMaterial.emissive.clone(),
              emissiveIntensity: oldMaterial.emissiveIntensity,
              metalness: 0.7,
              roughness: 0.4,
              envMap: window.renderManager ? window.renderManager.envMap : null,
              envMapIntensity: 0.8,
              clearcoat: 0.6,
              clearcoatRoughness: 0.2,
              reflectivity: 0.8
            });
            nodeObject.material.dispose();
            nodeObject.material = newMaterial;
          }
        }
      }
      
      // Add atmospheric effects if they don't already exist
      const nodeRadius = nodeObject.geometry.parameters?.radius || 30;
      
      // Remove existing atmospheric layers before adding new ones to prevent duplicates
      if (nodeObject.userData.atmosphereLayers) {
        for (const layer of nodeObject.userData.atmosphereLayers) {
          if (layer && layer.parent === nodeObject) {
            if (layer.material) layer.material.dispose();
            if (layer.geometry) layer.geometry.dispose();
            nodeObject.remove(layer);
          }
        }
      }
      
      this.addAtmosphericEffects(nodeObject, color, nodeRadius);
      
      // Add or update cloud layer
      if (nodeObject.userData.cloudMesh) {
        nodeObject.remove(nodeObject.userData.cloudMesh);
        nodeObject.userData.cloudMesh.material.dispose();
        nodeObject.userData.cloudMesh.geometry.dispose();
        nodeObject.userData.cloudMesh = null;
      }
      
      this.addCloudLayer(nodeObject, color, window.renderManager ? window.renderManager.envMap : null);
    }
    
    addAtmosphericEffects(nodeObject, color, nodeRadius) {
     console.log('addAtmosphericEffects', nodeObject, color, nodeRadius);
      if (nodeObject.userData.atmosphereLayers) {
        for (const layer of nodeObject.userData.atmosphereLayers) {
          if (layer && layer.parent === nodeObject) {
            if (layer.material) layer.material.dispose();
            if (layer.geometry) layer.geometry.dispose();
            nodeObject.remove(layer);
          }
        }
      }
      
      const atmosphereLayers = [];
      
      // Add outer glow atmosphere
      const atmoGeometry = new THREE.SphereGeometry(nodeRadius * 1.3, 32, 32);
      const atmoMaterial = new THREE.MeshPhysicalMaterial({
        color: color,
        transparent: true,
        opacity: 0.15,
        side: THREE.BackSide,
        envMap: window.renderManager ? window.renderManager.envMap : null,
        envMapIntensity: 0.4,
        roughness: 1.0,
        metalness: 0.0,
        clearcoat: 0.0,
        transmission: 0.9,
        ior: 1.2
      });
      
      const atmosphereMesh = new THREE.Mesh(atmoGeometry, atmoMaterial);
      atmosphereMesh.userData.type = 'atmosphere';
      nodeObject.add(atmosphereMesh);
      atmosphereLayers.push(atmosphereMesh);
      
      // Add polar glow effect
      const polarGeometry = new THREE.RingGeometry(nodeRadius * 0.5, nodeRadius * 0.9, 32);
      const polarMaterial = new THREE.MeshBasicMaterial({
        color: new THREE.Color(color).multiplyScalar(1.5),
        transparent: true,
        opacity: 0.3,
        side: THREE.DoubleSide,
        blending: THREE.AdditiveBlending
      });
      
      const polarRing = new THREE.Mesh(polarGeometry, polarMaterial);
      polarRing.rotation.x = Math.PI / 2;
      polarRing.position.y = nodeRadius * 0.7;
      polarRing.userData.type = 'polarRing';
      nodeObject.add(polarRing);
      atmosphereLayers.push(polarRing);
      
      // Store atmosphere references for animation and cleanup
      nodeObject.userData.atmosphereLayers = atmosphereLayers;
      console.log('addAtmosphericEffects', nodeObject, color, nodeRadius);
      return atmosphereLayers;
    }
    
    addCloudLayer(nodeObject, color, envMap) {
      console.log('addCloudLayer', nodeObject, color, envMap);
      // Only add if not already present
      if (!nodeObject.userData.cloudMesh) {
        const nodeRadius = nodeObject.geometry.parameters?.radius || 30;
        const cloudGeometry = new THREE.SphereGeometry(nodeRadius * 1.05, 24, 24);
        const cloudMaterial = new THREE.MeshPhysicalMaterial({
          color: 0xffffff,
          transparent: true,
          opacity: 0.4,
          alphaMap: this.getCloudTexture(),
          envMap: envMap,
          roughness: 1.0,
          metalness: 0.0,
          transmission: 0.2
        });
        
        const cloudMesh = new THREE.Mesh(cloudGeometry, cloudMaterial);
        cloudMesh.rotation.y = Math.random() * Math.PI * 2;
        cloudMesh.rotation.x = Math.random() * Math.PI * 0.2;
        cloudMesh.userData.type = 'cloudLayer';
        nodeObject.add(cloudMesh);
        nodeObject.userData.cloudMesh = cloudMesh;
        
        // Add cloud rotation animation data
        nodeObject.userData.cloudRotationSpeed = 0.0003 + Math.random() * 0.0005;
        nodeObject.userData.cloudRotationAxis = new THREE.Vector3(
          Math.random() * 0.2 - 0.1,
          1,
          Math.random() * 0.2 - 0.1
        ).normalize();
      }
    }

    updateQualityAnimations(time) {
      // Skip if no nodes or animations are disabled
      if (this.nodeObjects.size === 0) return;
    
      // Animate all nodes with quality effects
      for (const [nodeId, nodeObject] of this.nodeObjects.entries()) {
        // Skip nodes without quality effects
        if (!nodeObject.userData.qualityApplied) continue;
        
        // Animate atmosphere layers
        if (nodeObject.userData.atmosphereLayers) {
          for (const layer of nodeObject.userData.atmosphereLayers) {
            if (layer.userData.type === 'polarRing') {
              // Rotate polar rings
              layer.rotation.z = time * 0.2;
            }
          }
        }
        
        // Animate cloud layers
        if (nodeObject.userData.cloudMesh) {
          // Rotate clouds slowly
          nodeObject.userData.cloudMesh.rotation.y = time * 0.05;
          
          // Pulse cloud opacity for visual effect
          const pulseFactor = 0.7 + Math.sin(time * 0.3) * 0.1;
          if (nodeObject.userData.cloudMesh.material) {
            nodeObject.userData.cloudMesh.material.opacity = 0.4 * pulseFactor;
          }
        }
      }
    }
    
    generateCloudTexture(color = 0xffffff) {
      const canvas = document.createElement('canvas');
      const ctx = canvas.getContext('2d');
      canvas.width = 512;
      canvas.height = 512;
      
      // Clear canvas
      ctx.fillStyle = 'rgba(0,0,0,0)';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      
      // Convert color to RGB
      const r = (color >> 16) & 255;
      const g = (color >> 8) & 255;
      const b = color & 255;
      
      // Draw cloud-like patterns
      const cloudColor = `rgba(${r}, ${g}, ${b}, 0.1)`;
      ctx.fillStyle = cloudColor;
      
      // Generate random cloud shapes
      const numShapes = 50;
      for (let i = 0; i < numShapes; i++) {
        const x = Math.random() * canvas.width;
        const y = Math.random() * canvas.height;
        const radius = 20 + Math.random() * 60;
        
        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        ctx.fill();
      }
      
      // Apply Gaussian blur for smoother clouds
      ctx.filter = 'blur(16px)';
      ctx.drawImage(canvas, 0, 0);
      ctx.filter = 'none';
      
      // Create texture from canvas
      const texture = new THREE.CanvasTexture(canvas);
      texture.needsUpdate = true;
      
      return texture;
    }

    getCloudTexture() {
      // Cache the cloud texture to avoid regenerating it every time
      if (!this._cloudTextureCache) {
        this._cloudTextureCache = this.generateCloudTexture();
      }
      return this._cloudTextureCache;
    }

    forceApplyQualityToAllNodes() {
      // Check if renderManager exists and get the current quality mode
      const isQualityMode = window.currentQualityMode === 'quality';

      console.log(`wtf ${window.currentQualityMode}`);
      
      console.log(`Force applying quality mode to all nodes: ${isQualityMode ? 'quality' : 'standard'}`);
      
      // Process all nodes
      for (const [nodeId, nodeObject] of this.nodeObjects.entries()) {
        const nodeData = this.nodes.get(nodeId);
        if (!nodeData || !nodeObject) continue;
        
        // Reset quality flags to ensure proper reapplication
        nodeObject.userData.qualityApplied = false;
        
        // Clean up any existing atmospheric effects
        this.cleanupQualityEffects(nodeObject);
        
        // Apply materials based on LOD level and quality mode
        const distance = this.camera.position.distanceTo(nodeObject.position);
        
        if (distance > 10000) {  // Far LOD threshold
          nodeObject.userData.currentLOD = 'far';
          this.applyFarLOD(nodeObject, nodeData, 20 + Math.min(70, nodeData.connections * 5), isQualityMode);
        } 
        else if (distance > 3000) {  // Medium LOD threshold
          nodeObject.userData.currentLOD = 'medium';
          this.applyMediumLOD(nodeObject, nodeData, 20 + Math.min(70, nodeData.connections * 5), isQualityMode);
        }
        else {  
          nodeObject.userData.currentLOD = 'close';
          this.applyCloseLOD(nodeObject, nodeData, 20 + Math.min(70, nodeData.connections * 5), isQualityMode);
        }
        
        // For quality mode, ensure effects are applied
        if (isQualityMode) {
          this.applyNodeQualityEffects(nodeObject, nodeData);
          nodeObject.userData.qualityApplied = true;
        }
      }
    }
    
    // Add helper function to clean up quality effects
    cleanupQualityEffects(nodeObject) {
      // Remove atmosphere layers
      if (nodeObject.userData.atmosphereLayers) {
        for (const layer of nodeObject.userData.atmosphereLayers) {
          if (layer && layer.parent === nodeObject) {
            if (layer.material) layer.material.dispose();
            if (layer.geometry) layer.geometry.dispose();
            nodeObject.remove(layer);
          }
        }
        nodeObject.userData.atmosphereLayers = [];
      }
      
      // Remove cloud mesh
      if (nodeObject.userData.cloudMesh) {
        nodeObject.remove(nodeObject.userData.cloudMesh);
        if (nodeObject.userData.cloudMesh.material) {
          nodeObject.userData.cloudMesh.material.dispose();
        }
        if (nodeObject.userData.cloudMesh.geometry) {
          nodeObject.userData.cloudMesh.geometry.dispose();
        }
        nodeObject.userData.cloudMesh = null;
      }
    }
  }

