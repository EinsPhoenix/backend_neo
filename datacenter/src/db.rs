use log::{info, error};
use neo4rs::{Graph, Error as Neo4jError};
use std::env;
use tokio::sync::OnceCell;
use std::fmt;

pub static DB: OnceCell<DatabaseCluster> = OnceCell::const_new();

#[derive(Debug)]
#[allow(dead_code)]
pub enum DbError {
    Neo4jError(Neo4jError),
    ConnectionError(String),
    OtherError(String),
}

impl From<Neo4jError> for DbError {
    fn from(error: Neo4jError) -> Self {
        DbError::Neo4jError(error)
    }
}

impl From<String> for DbError {
    fn from(error: String) -> Self {
        DbError::ConnectionError(error)
    }
}

pub async fn initialize_db() -> Result<DatabaseCluster, DbError> {
    info!("initialize_db called");
    dotenv::dotenv().ok();

    let uri_server1 = env::var("NEO4J_URI_WRITE_1").map_err(|e| {
        error!("Failed to read NEO4J_URI_WRITE_1: {}", e);
        DbError::ConnectionError(e.to_string())
    })?;
    let uri_server2 = env::var("NEO4J_URI_WRITE_2").map_err(|e| {
        error!("Failed to read NEO4J_URI_WRITE_2: {}", e);
        DbError::ConnectionError(e.to_string())
    })?;
    let uri_server3 = env::var("NEO4J_URI_READ_3").map_err(|e| {
        error!("Failed to read NEO4J_URI_READ_3: {}", e);
        DbError::ConnectionError(e.to_string())
    })?;

    let primary_nodes = vec![
        connect_db(uri_server1).await?,
    ];
    let secondary_nodes = vec![
        connect_db(uri_server3).await?,
        connect_db(uri_server2).await?,
    ];

    let cluster = DatabaseCluster {
        primary_nodes,
        secondary_nodes,
    };
    info!("initialize_db returning: {:?}", cluster);
    Ok(cluster)
}

async fn connect_db(uri_env: String) -> Result<Graph, DbError> {
    info!("Attempting to connect to Neo4j at {}", uri_env);
    let username = env::var("DATABASE_USER").map_err(|e| {
        error!("DATABASE_USER env variable is missing: {}", e);
        DbError::ConnectionError("Missing DATABASE_USER".into())
    })?;
    let password = env::var("DATABASE_PASSWORD").map_err(|e| {
        error!("DATABASE_PASSWORD env variable is missing: {}", e);
        DbError::ConnectionError("Missing DATABASE_PASSWORD".into())
    })?;

    let graph = Graph::new(&uri_env, &username, &password).await.map_err(|e| {
        error!("Connection to Neo4j failed at {}: {:?}", uri_env, e);
        DbError::Neo4jError(e)
    })?;

    info!("Successfully connected to Neo4j at {}", uri_env);
    Ok(graph)
}


pub async fn get_db() -> Result<&'static Graph, DbError> {
    info!("get_db called");
    if let Some(cluster) = DB.get() {
        info!("get_db returning primary node");
        return Ok(&cluster.primary_nodes[0]);
    }

    let cluster = initialize_db().await?;
    DB.set(cluster).map_err(|_| {
        error!("Failed to initialize DB");
        DbError::OtherError("Failed to initialize DB".into())
    })?;

    info!("get_db returning newly initialized primary node");
    Ok(&DB.get().unwrap().primary_nodes[0])
}

pub async fn get_read_db(number:usize) -> Result<&'static Graph, DbError> {
    info!("get_read_db called");
    if let Some(cluster) = DB.get() {
        info!("get_read_db returning secondary node");

        return Ok(&cluster.secondary_nodes[0]);
    }

    let cluster = initialize_db().await?;
    DB.set(cluster).map_err(|_| {
        error!("Failed to initialize DB");
        DbError::OtherError("Failed to initialize DB".into())
    })?;

    info!("get_read_db returning newly initialized secondary node");
    Ok(&DB.get().unwrap().secondary_nodes[number])
}

#[derive(Clone)]
pub struct DatabaseCluster {
    primary_nodes: Vec<Graph>,
    secondary_nodes: Vec<Graph>,
}

impl std::fmt::Debug for DatabaseCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseCluster")
            .field("primary_nodes_count", &self.primary_nodes.len())
            .field("secondary_nodes_count", &self.secondary_nodes.len())
            .finish()
    }
}




impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Neo4jError(e) => write!(f, "Neo4j error: {}", e),
            DbError::ConnectionError(e) => write!(f, "Connection error: {}", e),
            DbError::OtherError(e) => write!(f, "Error: {}", e),
        }
    }
}