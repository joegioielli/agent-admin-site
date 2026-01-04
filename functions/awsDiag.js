// functions/awsDiag.js
// Diagnostic: use YOUR custom env creds (MY_AWS_*) explicitly, show identity,
// and test PutObject to photos-incoming.

import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const REGION = process.env.MY_AWS_REGION || "us-east-2";
const BUCKET = process.env.MY_AWS_BUCKET || "gioi-real-estate-bucket";

const accessKeyId = process.env.MY_AWS_ACCESS_KEY_ID || "";
const secretAccessKey = process.env.MY_AWS_SECRET_ACCESS_KEY || "";
const hasCustomCreds = Boolean(accessKeyId && secretAccessKey);

const commonClientOpts = hasCustomCreds
  ? { region: REGION, credentials: { accessKeyId, secretAccessKey } }
  : { region: REGION }; // will fall back to Netlify's role

const sts = new STSClient(commonClientOpts);
const s3  = new S3Client(commonClientOpts);

export const handler = async () => {
  let identity = {};
  let canPutIncoming = false;
  let putError = null;

  try {
    const out = await sts.send(new GetCallerIdentityCommand({}));
    identity = { account: out.Account, userId: out.UserId, arn: out.Arn };
  } catch (e) {
    identity = { error: String(e?.message || e) };
  }

  const key = `photos-incoming/diag-${Date.now()}.txt`;
  try {
    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: "diag-ok",
      ServerSideEncryption: "AES256"
    }));
    canPutIncoming = true;
  } catch (e) {
    canPutIncoming = false;
    putError = { name: e?.name, code: e?.code || e?.Code, message: e?.message || String(e) };
  }

  return {
    statusCode: 200,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      ok: true,
      region: REGION,
      bucket: BUCKET,
      usingCustomCreds: hasCustomCreds,
      identity,
      canPutIncoming,
      testKey: key,
      putError
    }, null, 2)
  };
};
