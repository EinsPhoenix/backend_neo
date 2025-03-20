class CameraController {
    constructor(camera, domElement) {
        this.camera = camera;
        this.domElement = domElement;
        
        this.keys = {
            w: false,
            a: false,
            s: false,
            d: false,
            SHIFT: false
        };
        
        this.moveSpeed = 5000;
        this.speedMultiplier = 1.0; 

        // Maussteuerung
        this.mouseX = 0;
        this.mouseY = 0;
        this.yaw = 0;
        this.pitch = 0;
        this.mouseSensitivity = 0.2;
        
        this.popupFadeTimer = 0;
        this.createSpeedPopup();
        
        
        document.addEventListener('pointerlockchange', this.onPointerLockChange.bind(this), false);
        
   
        this.addEventListeners();
    }
    
    createSpeedPopup() {
        this.speedPopup = document.getElementById('speedPopup');
        this.hidePopupTimeout = null;
    }
    
    updatePopupTexture() {
        const ctx = this.popupContext;
      
        ctx.clearRect(0, 0, this.popupCanvas.width, this.popupCanvas.height);
      
        ctx.fillStyle = "rgba(0, 0, 0, 0.5)";
        ctx.fillRect(0, 0, this.popupCanvas.width, this.popupCanvas.height);
       
        ctx.font = "30px sans-serif";
        ctx.fillStyle = "white";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(`Speed: ${this.speedMultiplier.toFixed(2)}`, this.popupCanvas.width / 2, this.popupCanvas.height / 2);
      
        this.popupTexture.needsUpdate = true;
    }
    
    addEventListeners() {
      
        this.domElement.addEventListener('mousemove', this.onMouseMove.bind(this));
        
     
        this.domElement.addEventListener('mousedown', this.onMouseDown.bind(this));
        this.domElement.addEventListener('mouseup', this.onMouseUp.bind(this));
        
      
        this.domElement.addEventListener('wheel', this.onWheel.bind(this));
        
     
        window.addEventListener('keydown', this.onKeyDown.bind(this));
        window.addEventListener('keyup', this.onKeyUp.bind(this));
        
        
        this.domElement.addEventListener('contextmenu', (e) => e.preventDefault());
    }
    
    onPointerLockChange() {
        if (document.pointerLockElement === this.domElement) {
         
        } else {
            
            this.domElement.style.cursor = "default";
        }
    }
    
    onMouseDown(event) {
        if (event.button === 2) { 
            
            this.domElement.requestPointerLock();
            event.preventDefault();
        }
    }
    
    onMouseUp(event) {
        if (event.button === 2) { 
            
            if (document.pointerLockElement === this.domElement) {
                document.exitPointerLock();
            }
        }
    }
    
    onMouseMove(event) {
        if (document.pointerLockElement !== this.domElement) {
           
            this.mouseX = (event.clientX / window.innerWidth) * 2 - 1;
            this.mouseY = -(event.clientY / window.innerHeight) * 2 + 1;
        }
    
        if (document.pointerLockElement === this.domElement) {
            const movementX = event.movementX || 0;
            const movementY = event.movementY || 0;
            
            this.yaw   -= movementX * this.mouseSensitivity * 0.01;
            this.pitch -= movementY * this.mouseSensitivity * 0.01;
            this.pitch = Math.max(-Math.PI / 2, Math.min(Math.PI / 2, this.pitch));
            
            this.updateCameraDirection();
        }
    }
    
    onWheel(event) {
      
        if (document.pointerLockElement !== this.domElement) return;
        event.preventDefault();
        
        if (event.deltaY < 0) {
            this.speedMultiplier += 0.1;
            if (this.speedMultiplier > 10) {
                this.speedMultiplier = 10;
            }
        } else if (event.deltaY > 0) { 
            this.speedMultiplier -= 0.1;
            if (this.speedMultiplier < 0.1) {
                this.speedMultiplier = 0.1;
            }
        }
        
      
        this.showSpeedPopup();
        console.log("Speed Multiplier: ", this.speedMultiplier);
    }
    
    showSpeedPopup() {
      
        this.speedPopup.textContent = `Speed: ${this.speedMultiplier.toFixed(2)}`;
        
     
        clearTimeout(this.hidePopupTimeout);
        this.speedPopup.style.opacity = '0.8';
      
        this.hidePopupTimeout = setTimeout(() => {
            this.speedPopup.style.opacity = '0';
        }, 2000);
    }

    
    onKeyDown(event) {
        const key = event.key.toLowerCase();
        if (this.keys.hasOwnProperty(key)) {
            this.keys[key] = true;
        }
        if (event.shiftKey) {
            this.keys.SHIFT = true;
        }
    }
    
    onKeyUp(event) {
        const key = event.key.toLowerCase();
        if (this.keys.hasOwnProperty(key)) {
            this.keys[key] = false;
        }
        if (!event.shiftKey) {
            this.keys.SHIFT = false;
        }
    }
    
    updateCameraDirection() {
        const direction = new THREE.Vector3();
        direction.x = Math.sin(this.yaw) * Math.cos(this.pitch);
        direction.y = Math.sin(this.pitch);
        direction.z = Math.cos(this.yaw) * Math.cos(this.pitch);
        this.camera.lookAt(this.camera.position.clone().add(direction));
    }
    
    update(delta) {
     
        if (this.popupFadeTimer > 0) {
            this.popupFadeTimer -= delta;
            let newOpacity = this.popupFadeTimer / 2.0;
            this.speedPopupSprite.material.opacity = newOpacity;
        }
        
        const baseSpeed = this.keys.SHIFT ? this.moveSpeed * 4 : this.moveSpeed;
        
        const speed = baseSpeed * this.speedMultiplier;
        const actualSpeed = speed * delta;
        
        const direction = new THREE.Vector3();
        this.camera.getWorldDirection(direction);
        
        const forwardVector = direction.clone().normalize().multiplyScalar(actualSpeed);
        const rightVector = new THREE.Vector3().crossVectors(direction, this.camera.up).normalize().multiplyScalar(actualSpeed);
        
        if (this.keys.w) {
            this.camera.position.add(forwardVector);
        }
        if (this.keys.s) {
            this.camera.position.sub(forwardVector);
        }
        if (this.keys.a) {
            this.camera.position.sub(rightVector);
        }
        if (this.keys.d) {
            this.camera.position.add(rightVector);
        }
    }
    
    isControlActive() {
        return document.pointerLockElement === this.domElement;
    }
    
    getMouseCoordinates() {
        return { x: this.mouseX, y: this.mouseY };
    }
    
}
