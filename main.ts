console.log("Worker starting...");
Deno.serve(() => new Response("OK"));
