use amqprs::{
    connection::{
        Connection,
        OpenConnectionArguments
    },
    callbacks::{
        DefaultConnectionCallback,
        DefaultChannelCallback
    },
    channel::{
        QueueDeclareArguments,
        BasicConsumeArguments,
        BasicAckArguments
    }
};
use tokio::{
    io::Error as TError,
    sync::Notify
};
use json::JsonValue;
use lettre::{Message, message::SinglePart, SmtpTransport, Transport, transport::smtp::authentication::Credentials, message::header::ContentType};

use dotenvy::dotenv;
use std::{time::Duration, thread, env};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Configuration {
    rmq_user: String,
    rmq_password: String,
    rmq_address: String,
    rmq_port: u16,
    smtp_user: String,
    smtp_password: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<TError>> {
    dotenv().expect(".env file not found");
    let config: Configuration = envy::from_env::<Configuration>().unwrap();
    
    let conn = Connection::open(&OpenConnectionArguments::new(
        config.rmq_address.as_str(),
        config.rmq_port,
        config.rmq_user.as_str(),
        config.rmq_password.as_str())
    ).await.unwrap();

    conn.register_callback(DefaultConnectionCallback).await.unwrap();
    
    let ch = conn.open_channel(None).await.unwrap();
    ch.register_callback(DefaultChannelCallback).await.unwrap();
    
    let q_name = "task_queue";
    let q_args = QueueDeclareArguments::new(q_name).durable(true).finish();
    let (_, _, _) = ch.queue_declare(q_args).await.unwrap().unwrap();
    
    let consumer_args = BasicConsumeArguments::default().queue(String::from(q_name)).finish();
    let (_ctag, mut rx) = ch.basic_consume_rx(consumer_args).await.unwrap();

    tokio::spawn(async move {
        println!("[*] Waiting for messages. To exit press CTRL+C");
        while let Some(msg) = rx.recv().await {
            if let Some(payload) = msg.content {
                // TODO: Check that all keys are valid
                let message = json::parse(std::str::from_utf8(&payload).unwrap()).unwrap();
                println!("[x] Received: Sending email to {:?}", message["to"]);
                send_email_smtp(message).await;
                println!("[x] Email Sent...");
                ch.basic_ack(BasicAckArguments::new(msg.deliver.unwrap().delivery_tag(), false)).await.unwrap();
            }
        }
    });

    let guard = Notify::new();
    guard.notified().await;

    Ok(())
}

async fn send_email_smtp(
    message: JsonValue
) -> Result<(), Box<dyn std::error::Error>> {
    dotenv().expect(".env file not found");
    let config = envy::from_env::<Configuration>().unwrap();

    let email = Message::builder()
    .from(message["from"].to_string().as_str().parse().unwrap()) 
    .to(message["to"].to_string().as_str().parse().unwrap()) 
    .subject(message["subject"].to_string().as_str())
    .singlepart(
        SinglePart::html(message["body"].to_string())
    )
    .unwrap();

    let creds = Credentials::new(config.smtp_user, config.smtp_password);
    
    let mailer = SmtpTransport::relay("smtpout.secureserver.net") 
    .unwrap() 
    .credentials(creds) 
    .build(); 

    match mailer.send(&email) { 
        Ok(_) => println!("Email sent successfully!"), 
        Err(e) => panic!("Could not send email: {:?}", e), 
    }

    Ok(())
}

