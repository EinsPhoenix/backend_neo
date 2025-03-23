// render-manager.js
class RenderManager {
    constructor(renderer, camera, scene) {
      this.renderer = renderer;
      this.camera = camera;
      this.scene = scene;
      this.composer = null;
      this.bloomPass = null;
      this.ssaoPass = null;
      this.loadingIndicator = document.getElementById('loading');
      this.disableAnimations = false;
      this.disableNiceMeshes = false;
      this.defaultFarPlane = 20000;
      this.renderDistanceController = null;
      this.spaceSize = 50000;
      this.useLOD = true;
      this.useInstancedRendering = false;
      this.lastFPSUpdate = 0;
      this.frameCount = 0;
      this.currentFPS = 0;
      this.fpsDisplay = document.createElement('div');
      this.setupFPSDisplay();
      this.envMap = null;
      this.qualityAnimationTime = 0;
    }
  
    init() {
      try {
        this.setupBloomEffect();
        this.setupEnvironmentMap();
      } catch (error) {
        console.error("Konnte Effekte nicht initialisieren:", error);
      }
  
      this.setupGUI();

      window.objectManager.setUseLOD(this.useLOD);
    }
    
    setupEnvironmentMap() {
      const pmremGenerator = new THREE.PMREMGenerator(this.renderer);
      pmremGenerator.compileEquirectangularShader();
      
      const intensity = 1.0;
      const colors = [
        new THREE.Color(0x0077ff).multiplyScalar(intensity),
        new THREE.Color(0xff0077).multiplyScalar(intensity),
        new THREE.Color(0x0000ff).multiplyScalar(intensity),
      ];
      
      const cubeRenderTarget = this.generateEnvironmentCubemap(colors, pmremGenerator);
      this.envMap = cubeRenderTarget.texture;
    }
    
    generateEnvironmentCubemap(colors, pmremGenerator) {
      const size = 256;
      const cubeRenderTarget = new THREE.WebGLCubeRenderTarget(size);
      const cubecam = new THREE.CubeCamera(1, 1000, cubeRenderTarget);
      
      const scene = new THREE.Scene();
      
      // Create a spherical environment with gradients
      const geometry = new THREE.SphereGeometry(100, 32, 32);
      const material = new THREE.ShaderMaterial({
        uniforms: {
          color1: { value: colors[0] },
          color2: { value: colors[1] },
          color3: { value: colors[2] }
        },
        vertexShader: `
          varying vec3 vWorldPosition;
          void main() {
            vec4 worldPosition = modelMatrix * vec4(position, 1.0);
            vWorldPosition = worldPosition.xyz;
            gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
          }
        `,
        fragmentShader: `
          uniform vec3 color1;
          uniform vec3 color2;
          uniform vec3 color3;
          varying vec3 vWorldPosition;
          
          void main() {
            vec3 viewDirection = normalize(vWorldPosition);
            float t = viewDirection.y * 0.5 + 0.5;
            float s = viewDirection.x * 0.5 + 0.5;
            vec3 color = mix(mix(color1, color2, s), color3, t);
            gl_FragColor = vec4(color, 1.0);
          }
        `,
        side: THREE.BackSide
      });
      
      const mesh = new THREE.Mesh(geometry, material);
      scene.add(mesh);
      
      cubecam.update(this.renderer, scene);
      
      return pmremGenerator.fromCubemap(cubeRenderTarget.texture);
    }

    setupGUI() {
      const gui = new dat.GUI();
      const renderOptions = gui.addFolder('Render Optionen');
      const performanceOptions = gui.addFolder('Performance Optionen');

      const params = {
          renderQuality: 'Standard',
          renderDistance: this.defaultFarPlane,
          spaceSize: this.spaceSize,
          useLOD: this.useLOD,
          showFPS: true,
          dynamicRendering: true,
          edgeBundling: true,
          bundlingStrength: 0.5,
          adaptivePerformance: true,
          performanceMode: false
      };

      renderOptions.add(params, 'renderQuality', ['Qualität', 'Standard', 'Performance'])
          .name('Anzeigemodus')
          .onChange((value) => this.applyRenderOption(value));

      this.renderDistanceController = renderOptions.add(params, 'renderDistance', 1000, 1000000)
          .name('Renderdistanz')
          .onChange((value) => this.setRenderDistance(value));

      this.spaceSizeController = renderOptions.add(params, "spaceSize", 1000, 1000000)
         .name("Raumgröße")
         .onChange((value) => this.setSpaceSize(value));

      renderOptions.add(params, "showFPS")
          .name("FPS anzeigen")
          .onChange((value) => {
            this.fpsDisplay.style.display = value ? 'block' : 'none';
          });
          
      renderOptions.add(params, "dynamicRendering")
          .name("Dynamisches Rendering")
          .onChange((value) => {
            if (window.objectManager) {
              window.objectManager.setDynamicRendering(value);
            }
          });
          
      renderOptions.add(params, "edgeBundling")
          .name("Kanten bündeln")
          .onChange((value) => {
            if (window.objectManager) {
              window.objectManager.toggleEdgeBundling(value, params.bundlingStrength);
            }
          });
          
      renderOptions.add(params, "bundlingStrength", 0.1, 1.0)
          .name("Bündelungsstärke")
          .onChange((value) => {
            if (window.objectManager && window.objectManager.edgeBundlingEnabled) {
              window.objectManager.toggleEdgeBundling(true, value);
            }
          });

     
      performanceOptions.add(params, "adaptivePerformance")
          .name("Adaptive Performance")
          .onChange((value) => {
            window.useAdaptivePerformance = value;
            this.updatePerformanceStatus();
          });
          
      this.performanceModeController = performanceOptions.add(params, "performanceMode")
          .name("Performance Modus")
          .onChange((value) => {
              if (window.objectManager) {
                  window.objectManager.setPerformanceMode(value);
              }
              this.updatePerformanceStatus();
          });

      renderOptions.open();
      performanceOptions.open();
      
      this.applyRenderOption(params.renderQuality);
      
      if (window.objectManager) {
        window.objectManager.setDynamicRendering(params.dynamicRendering);
        window.objectManager.toggleEdgeBundling(params.edgeBundling, params.bundlingStrength);
      }
      
      window.useAdaptivePerformance = params.adaptivePerformance;
      if (window.objectManager) {
        window.objectManager.setPerformanceMode(params.performanceMode);
      }
      
      this.createPerformanceStatusDisplay();
      this.updatePerformanceStatus();
    }

    createPerformanceStatusDisplay() {
      this.performanceStatus = document.createElement('div');
      this.performanceStatus.style.position = 'absolute';
      this.performanceStatus.style.top = '40px';  
      this.performanceStatus.style.left = '5px';
      this.performanceStatus.style.color = 'white';
      this.performanceStatus.style.fontFamily = 'monospace';
      this.performanceStatus.style.zIndex = '1000';
      this.performanceStatus.style.backgroundColor = 'rgba(0,0,0,0.5)';
      this.performanceStatus.style.padding = '5px';
      this.performanceStatus.style.borderRadius = '3px';
      document.body.appendChild(this.performanceStatus);
    }

    updatePerformanceStatus() {
      if (!this.performanceStatus) return;
      
      const adaptiveMode = window.useAdaptivePerformance ? 'AN' : 'AUS';
      const performanceMode = window.objectManager && window.objectManager.getPerformanceIsEnabled() ? 'AN' : 'AUS';
      
      this.performanceStatus.innerHTML = 
          `Adaptive: <span style="color:${window.useAdaptivePerformance ? '#4CAF50' : '#F44336'}">${adaptiveMode}</span> | ` +
          `Perf-Modus: <span style="color:${performanceMode === 'AN' ? '#F44336' : '#4CAF50'}">${performanceMode}</span>`;
    }
     
    setRenderDistance(distance) {
      if (this.camera.far === distance) return; 
  
      this.camera.far = distance;
      this.camera.updateProjectionMatrix();
  
      if (this.scene.fog) {
          this.scene.fog.far = distance * 0.9;
      }
      
      if (this.renderDistanceController && this.renderDistanceController.getValue() !== distance) {
          this.renderDistanceController.setValue(distance);
      }
    }

    setSpaceSize(spaceSize) {
      this.spaceSize = spaceSize;
      this.updateEventBusHandler();
    }

    updateEventBusHandler() {
      if (window.eventBus) {
        window.eventBus.dispatchEvent(new CustomEvent('renderOptionChanged', {
          detail: {
            disableAnimations: this.disableAnimations,
            disableNiceMeshes: this.disableNiceMeshes,
            spaceSize: this.spaceSize,
            useLOD: this.useLOD
          }
        }));
      }
    }
    
    applyRenderOption(option) {
      switch(option) {
        case 'Qualität':
          if (this.composer && this.bloomPass) {
            this.bloomPass.strength = 1.25;
            this.bloomPass.radius = 0.6;
            this.bloomPass.threshold = 0.85;
          }
          
          this.setupSSAO();
          
          this.renderer.shadowMap.enabled = true;
          this.renderer.shadowMap.type = THREE.PCFSoftShadowMap;
          this.renderer.antialias = true;
          this.renderer.setPixelRatio(window.devicePixelRatio);
          
          this.scene.children.forEach(child => {
            if (child.geometry instanceof THREE.CircleGeometry) {
              child.visible = true;
            }
          });
          
          this.useLOD = true;
          if (window.objectManager) {
            window.objectManager.setUseLOD(true);
            window.objectManager.setPerformanceMode(false);
            window.objectManager.applyQualityMaterials(this.envMap);
            window.objectManager.enableEdgeGlow(true);
          }
          
          this.setRenderDistance(500000);
          
          this.displayAllLabels(true);
          
          this.disableAnimations = false;
          this.disableNiceMeshes = false;
          
          this.loadingIndicator.textContent = 'Qualitätsmodus: Bloom, SSAO, Umgebungsspiegelung und erweiterte Beleuchtung aktiviert';
          
          if (window.addStarsToScene) window.addStarsToScene(true, true); 
          
          break;
          
        case 'Standard':
          if (this.composer && this.bloomPass) {
            this.bloomPass.strength = 0;
            this.bloomPass.radius = 0;
            this.bloomPass.threshold = 1;
          }
          
          // Disable SSAO in standard mode
          if (this.ssaoPass) {
            this.ssaoPass.enabled = false;
          }
          
          this.renderer.shadowMap.enabled = true;
          this.renderer.shadowMap.type = THREE.PCFSoftShadowMap;
          this.renderer.antialias = false;
          this.renderer.setPixelRatio(window.devicePixelRatio);
          
          this.scene.children.forEach(child => {
            if (child.geometry instanceof THREE.CircleGeometry) {
              child.visible = true;
            }
          });
          
          this.useLOD = true;
          if (window.objectManager) {
            window.objectManager.setUseLOD(true);
            window.objectManager.setPerformanceMode(false);
            window.objectManager.applyStandardMaterials();
            window.objectManager.enableEdgeGlow(false);
          }
          
          this.displayAllLabels(true);
          
          this.disableAnimations = false;
          this.disableNiceMeshes = false;
          
          this.setRenderDistance(this.defaultFarPlane);
          if (window.addStarsToScene) window.addStarsToScene(false);
          
          this.loadingIndicator.textContent = 'Standardmodus: LOD aktiviert, Bloom deaktiviert';
          break;
          
        case 'Performance':
          if (this.composer && this.bloomPass) {
            this.bloomPass.strength = 0;
            this.bloomPass.radius = 0;
            this.bloomPass.threshold = 1;
          }
          
        
          if (this.ssaoPass) {
            this.ssaoPass.enabled = false;
          }
    
          this.renderer.shadowMap.enabled = false;
          this.renderer.antialias = false;
          this.renderer.setPixelRatio(1);
          
          this.scene.children.forEach(child => {
            if (child.geometry instanceof THREE.CircleGeometry) {
              child.visible = false;
            }
          });
    
          this.useLOD = true;
          if (window.objectManager) {
            window.objectManager.setUseLOD(true);
            window.objectManager.setPerformanceMode(true);
            window.objectManager.applyPerformanceMaterials();
            window.objectManager.enableEdgeGlow(false);
          }
    
          this.setRenderDistance(15000);
          
          this.displayAllLabels(false);
          
          this.disableAnimations = true;
          this.disableNiceMeshes = true;
          
          this.loadingIndicator.textContent = 'Performance-Modus: LOD und Instanced Rendering aktiviert';
          if (window.addStarsToScene) window.addStarsToScene(false);
          
          break;
      }
    
      this.updateEventBusHandler();
    }
    
    setupSSAO() {
      if (!this.composer) return;
      
      try {
        if (typeof THREE.SSAOPass === 'undefined') {
          console.warn('SSAO effect could not be initialized. Required dependencies missing.');
          return;
        }
        
        if (this.ssaoPass) {
          this.ssaoPass.enabled = true;
          return;
        }
        
        this.ssaoPass = new THREE.SSAOPass(this.scene, this.camera, window.innerWidth, window.innerHeight);
        this.ssaoPass.kernelRadius = 16;
        this.ssaoPass.minDistance = 0.005;
        this.ssaoPass.maxDistance = 0.1;
        
        this.composer.addPass(this.ssaoPass);
        
        console.log("SSAO effect successfully initialized");
      } catch (error) {
        console.error("Error initializing SSAO effect:", error);
      }
    }
  
    displayAllLabels(visible) {
      if (window.objectManager) {
        window.objectManager.nodeLabels.forEach(label => {
          label.visible = visible;
        });
        
        window.objectManager.lineLabels.forEach(labelInfo => {
          labelInfo.sprite.visible = visible;
        });
      }
    }
  
    setupBloomEffect() {
      try {
        if (typeof THREE.EffectComposer === 'undefined' || 
            typeof THREE.RenderPass === 'undefined' || 
            typeof THREE.UnrealBloomPass === 'undefined') {
            
          console.warn('Bloom effect could not be initialized. Required dependencies missing.');
          this.composer = null;
          return;
        }
        
        this.composer = new THREE.EffectComposer(this.renderer);
        
        const renderPass = new THREE.RenderPass(this.scene, this.camera);
        this.composer.addPass(renderPass);
        
        const bloomLayer = new THREE.Layers();
        bloomLayer.set(0); 
        
        this.bloomPass = new THREE.UnrealBloomPass(
          new THREE.Vector2(window.innerWidth, window.innerHeight),
          1.25,  
          0.6,   
          0.85   
        );
        
        this.composer.addPass(this.bloomPass);
        
        console.log("Bloom effect successfully initialized");
      } catch (error) {
        console.error("Error initializing bloom effect:", error);
        this.composer = null;
      }
    }
  
    onWindowResize() {
      this.camera.aspect = window.innerWidth / window.innerHeight;
      this.camera.updateProjectionMatrix();
      this.renderer.setSize(window.innerWidth, window.innerHeight);
      
      if (this.composer && this.composer.setSize) {
        try {
          this.composer.setSize(window.innerWidth, window.innerHeight);
        } catch (error) {
          console.error("Fehler beim Resize des Composers:", error);
        }
      }
      
      if (this.ssaoPass) {
        this.ssaoPass.setSize(window.innerWidth, window.innerHeight);
      }
    }
  
    render() {
      try {
   
        if (!this.disableAnimations && !this.disableNiceMeshes && window.objectManager) {
          this.qualityAnimationTime += 0.01;
          window.objectManager.updateQualityAnimations(this.qualityAnimationTime);
        }
        
       
        if (this.composer && this.composer.renderer) {
          this.composer.render();
        } else {
          this.renderer.render(this.scene, this.camera);
        }
        
        this.updateFPS();
      } catch (error) {
        console.error("Render error:", error);
        this.renderer.render(this.scene, this.camera);
      }
    }
  
    getDisableAnimations() {
      return this.disableAnimations;
    }
  
    getDisableNiceMeshes() {
      return this.disableNiceMeshes;
    }

    getRenderDistance() {
      return this.camera.far;
    }
      
    resetRenderDistance() {
      this.setRenderDistance(this.defaultFarPlane);
    }

    setupFPSDisplay() {
      this.fpsDisplay.style.position = 'absolute';
      this.fpsDisplay.style.top = '5px';
      this.fpsDisplay.style.left = '5px';
      this.fpsDisplay.style.color = 'white';
      this.fpsDisplay.style.fontFamily = 'monospace';
      this.fpsDisplay.style.zIndex = '1000';
      this.fpsDisplay.style.backgroundColor = 'rgba(0,0,0,0.5)';
      this.fpsDisplay.style.padding = '5px';
      this.fpsDisplay.style.borderRadius = '3px';
      document.body.appendChild(this.fpsDisplay);
    }

    // Optimize FPS counter to reduce overhead
    updateFPS() {
      this.frameCount++;
      const now = performance.now();
      
    
      if (now - this.lastFPSUpdate > 1000) {
        this.currentFPS = Math.round((this.frameCount * 1000) / (now - this.lastFPSUpdate));
        this.fpsDisplay.textContent = `FPS: ${this.currentFPS}`;
        this.frameCount = 0;
        this.lastFPSUpdate = now;
      }
    }
}
