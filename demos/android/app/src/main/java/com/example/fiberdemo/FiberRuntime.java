package com.example.fiberdemo;

import android.content.Context;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

public final class FiberRuntime {
    private static final String CONFIG_ASSET = "fiber_config.yml";
    private static final String CONFIG_FILE = "fiber_config.yml";
    private static final String CKB_KEY_FILE = "key";
    private static final String CKB_RPC_URL = "https://testnet.ckb.dev/";
    private static final String CKB_PREFS = "ckb_key";
    private static final String PREF_PUBKEY_HASH = "pubkey_hash";
    private static final String SECP256K1_LOCK_CODE_HASH =
            "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8";
    private static final BigInteger FIELD_PRIME =
            new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F", 16);
    private static final EcPoint GENERATOR = new EcPoint(
            new BigInteger("79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798", 16),
            new BigInteger("483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8", 16)
    );
    private static final BigInteger SECP256K1_ORDER =
            new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16);
    private static final BigInteger SHANNONS_PER_CKB = BigInteger.valueOf(100_000_000L);
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final long[] BLAKE2B_IV = {
            0x6a09e667f3bcc908L, 0xbb67ae8584caa73bL, 0x3c6ef372fe94f82bL, 0xa54ff53a5f1d36f1L,
            0x510e527fade682d1L, 0x9b05688c2b3e6c1fL, 0x1f83d9abfb41bd6bL, 0x5be0cd19137e2179L
    };
    private static final byte[][] BLAKE2B_SIGMA = {
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
            {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
            {11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
            {7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
            {9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
            {2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
            {12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
            {13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
            {6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
            {10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0},
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
            {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3}
    };
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final List<NativeEventListener> nativeEventListeners = new CopyOnWriteArrayList<>();
    private static final List<CkbPrepareListener> ckbPrepareListeners = new CopyOnWriteArrayList<>();
    private static boolean running;
    private static boolean ckbPreparing;
    private static boolean ckbReady;
    private static boolean nativeLibrariesLoaded;
    private static String nativeLoadError;

    private FiberRuntime() {
    }

    public static synchronized String prepareCkb(Context context) {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return loadError;
        }
        if (running) {
            return "CKB preparation skipped: Fiber is already running";
        }
        if (ckbPreparing) {
            return "CKB preparation already in progress";
        }

        try {
            File configFile = ensureConfigFile(context.getApplicationContext());
            File dataDir = new File(context.getFilesDir(), "fiber-data");
            if (!dataDir.exists() && !dataDir.mkdirs()) {
                return "CKB preparation failed: cannot create data directory";
            }
            if (!hasCkbKey(context)) {
                return "CKB preparation failed: CKB private key is not set";
            }

            ckbPreparing = true;
            ckbReady = false;
            String result = nativePrepareCkb(
                    configFile.getAbsolutePath(),
                    dataDir.getAbsolutePath(),
                    "info"
            );
            if (!result.equals("CKB preparation started")) {
                ckbPreparing = false;
            }
            return result;
        } catch (IOException exception) {
            ckbPreparing = false;
            return "CKB preparation failed: " + exception.getMessage();
        }
    }

    public static synchronized String start(Context context) {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return loadError;
        }

        try {
            File configFile = ensureConfigFile(context.getApplicationContext());
            File dataDir = new File(context.getFilesDir(), "fiber-data");
            if (!dataDir.exists() && !dataDir.mkdirs()) {
                return "Fiber start failed: cannot create data directory";
            }
            if (!hasCkbKey(context)) {
                return "Fiber start failed: CKB private key is not set";
            }

            String result = nativeStart(
                    configFile.getAbsolutePath(),
                    dataDir.getAbsolutePath(),
                    "info"
            );
            running = result.startsWith("Fiber started") || result.equals("Fiber already running");
            if (running) {
                ckbReady = false;
            }
            return result;
        } catch (IOException exception) {
            return "Fiber start failed: " + exception.getMessage();
        }
    }

    public static synchronized String stop() {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return loadError;
        }

        String result = nativeStop();
        running = false;
        ckbReady = false;
        return result;
    }

    public static synchronized NativeResult nodeInfo() {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return NativeResult.error(loadError);
        }
        return fromNativeCall(nativeNodeInfo());
    }

    public static synchronized NativeResult listPeers() {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return NativeResult.error(loadError);
        }
        return fromNativeCall(nativeListPeers());
    }

    public static synchronized NativeResult connectPeer(
            String address,
            String pubkey,
            String addrType,
            boolean save
    ) {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return NativeResult.error(loadError);
        }
        return fromNativeCall(nativeConnectPeer(
                emptyToNull(address),
                emptyToNull(pubkey),
                emptyToNull(addrType),
                save
        ));
    }

    public static synchronized NativeResult listChannels() {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return NativeResult.error(loadError);
        }
        return fromNativeCall(nativeListChannels());
    }

    public static synchronized NativeResult createChannel(String pubkey, String amount) {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return NativeResult.error(loadError);
        }
        try {
            return fromNativeCall(nativeOpenChannel(
                    requireTrimmed(pubkey, "pubkey"),
                    ckbAmountToShannonsHex(amount)
            ));
        } catch (IllegalArgumentException exception) {
            return NativeResult.error("Fiber createChannel failed: " + exception.getMessage());
        }
    }

    public static synchronized NativeResult shutdownChannel(String channelId) {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return NativeResult.error(loadError);
        }
        try {
            return fromNativeCall(nativeShutdownChannel(requireTrimmed(channelId, "channel_id"), true));
        } catch (IllegalArgumentException exception) {
            return NativeResult.error("Fiber shutdownChannel failed: " + exception.getMessage());
        }
    }

    public static synchronized NativeResult newInvoice(String amount, String description) {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return NativeResult.error(loadError);
        }
        try {
            return fromNativeCall(nativeNewInvoice(shannonsAmountToHex(amount), emptyToNull(description)));
        } catch (IllegalArgumentException exception) {
            return NativeResult.error("Fiber newInvoice failed: " + exception.getMessage());
        }
    }

    public static synchronized NativeResult sendPayment(String invoice) {
        String loadError = ensureNativeLibrariesLoaded();
        if (loadError != null) {
            return NativeResult.error(loadError);
        }
        try {
            return fromNativeCall(nativeSendPayment(requireTrimmed(invoice, "invoice")));
        } catch (IllegalArgumentException exception) {
            return NativeResult.error("Fiber sendPayment failed: " + exception.getMessage());
        }
    }

    public static synchronized boolean isRunning() {
        return running;
    }

    public static synchronized boolean isCkbPreparing() {
        return ckbPreparing;
    }

    public static synchronized boolean isCkbReady() {
        return ckbReady;
    }

    public static boolean hasCkbKey(Context context) {
        return getCkbKeyFile(context.getApplicationContext()).exists()
                && getSavedPubkeyHash(context.getApplicationContext()) != null;
    }

    public static String getSavedPubkeyHash(Context context) {
        String value = context.getApplicationContext()
                .getSharedPreferences(CKB_PREFS, Context.MODE_PRIVATE)
                .getString(PREF_PUBKEY_HASH, null);
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value;
    }

    public static CkbAccount setCkbPrivateKey(Context context, String privateKeyHex) throws IOException {
        String normalizedKey = normalizePrivateKey(privateKeyHex);
        String pubkeyHash = derivePubkeyHash(normalizedKey);

        File keyFile = getCkbKeyFile(context.getApplicationContext());
        File ckbDir = keyFile.getParentFile();
        if (ckbDir == null || (!ckbDir.exists() && !ckbDir.mkdirs())) {
            throw new IOException("cannot create ckb data directory");
        }
        try (FileOutputStream output = new FileOutputStream(keyFile)) {
            output.write((normalizedKey + "\n").getBytes(StandardCharsets.US_ASCII));
        }
        synchronized (FiberRuntime.class) {
            ckbReady = false;
        }

        context.getApplicationContext()
                .getSharedPreferences(CKB_PREFS, Context.MODE_PRIVATE)
                .edit()
                .putString(PREF_PUBKEY_HASH, pubkeyHash)
                .apply();
        return new CkbAccount(pubkeyHash);
    }

    public static CkbAccount previewCkbAccount(String privateKeyHex) throws IOException {
        return new CkbAccount(derivePubkeyHash(normalizePrivateKey(privateKeyHex)));
    }

    public static CkbBalance refreshCkbBalance(Context context) throws IOException, JSONException {
        String pubkeyHash = getSavedPubkeyHash(context.getApplicationContext());
        if (pubkeyHash == null) {
            throw new IOException("CKB private key is not set");
        }
        return refreshCkbBalance(pubkeyHash);
    }

    public static CkbBalance refreshCkbBalance(String pubkeyHash) throws IOException, JSONException {
        BigInteger shannons = queryCkbBalance(pubkeyHash);
        return new CkbBalance(pubkeyHash, shannons, formatCkb(shannons));
    }

    public static void addNativeEventListener(NativeEventListener listener) {
        nativeEventListeners.add(listener);
    }

    public static void removeNativeEventListener(NativeEventListener listener) {
        nativeEventListeners.remove(listener);
    }

    public static void addCkbPrepareListener(CkbPrepareListener listener) {
        ckbPrepareListeners.add(listener);
    }

    public static void removeCkbPrepareListener(CkbPrepareListener listener) {
        ckbPrepareListeners.remove(listener);
    }

    private static File ensureConfigFile(Context context) throws IOException {
        File configFile = new File(context.getFilesDir(), CONFIG_FILE);

        try (InputStream input = context.getAssets().open(CONFIG_ASSET);
             FileOutputStream output = new FileOutputStream(configFile)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
        }
        return configFile;
    }

    private static File getCkbKeyFile(Context context) {
        return new File(new File(new File(context.getFilesDir(), "fiber-data"), "ckb"), CKB_KEY_FILE);
    }

    @SuppressWarnings("unused")
    private static String randomSecretKeyHex() {
        byte[] key = new byte[32];
        BigInteger value;
        do {
            SECURE_RANDOM.nextBytes(key);
            value = new BigInteger(1, key);
        } while (value.signum() == 0 || value.compareTo(SECP256K1_ORDER) >= 0);

        char[] hex = new char[key.length * 2];
        for (int i = 0; i < key.length; i++) {
            int unsignedByte = key[i] & 0xff;
            hex[i * 2] = HEX[unsignedByte >>> 4];
            hex[i * 2 + 1] = HEX[unsignedByte & 0x0f];
        }
        return new String(hex);
    }

    private static String normalizePrivateKey(String privateKeyHex) throws IOException {
        if (privateKeyHex == null) {
            throw new IOException("private key is empty");
        }
        String value = privateKeyHex.trim();
        if (value.startsWith("0x") || value.startsWith("0X")) {
            value = value.substring(2);
        }
        if (value.length() != 64) {
            throw new IOException("private key must be 32 bytes hex");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (!hex) {
                throw new IOException("private key must be hex");
            }
        }
        BigInteger privateKey = new BigInteger(value, 16);
        if (privateKey.signum() == 0 || privateKey.compareTo(SECP256K1_ORDER) >= 0) {
            throw new IOException("private key is outside secp256k1 range");
        }
        return value.toLowerCase(Locale.US);
    }

    private static String derivePubkeyHash(String privateKeyHex) {
        EcPoint publicKey = multiply(GENERATOR, new BigInteger(privateKeyHex, 16));
        byte[] compressed = new byte[33];
        compressed[0] = publicKey.y.testBit(0) ? (byte) 0x03 : (byte) 0x02;
        byte[] x = toFixedBytes(publicKey.x, 32);
        System.arraycopy(x, 0, compressed, 1, x.length);

        byte[] hash = blake2b(compressed, 32, "ckb-default-hash".getBytes(StandardCharsets.US_ASCII));
        return "0x" + bytesToHex(Arrays.copyOf(hash, 20));
    }

    private static BigInteger queryCkbBalance(String pubkeyHash) throws IOException, JSONException {
        BigInteger total = BigInteger.ZERO;
        String cursor = null;
        do {
            JSONObject searchKey = new JSONObject()
                    .put("script", new JSONObject()
                            .put("code_hash", SECP256K1_LOCK_CODE_HASH)
                            .put("hash_type", "type")
                            .put("args", pubkeyHash))
                    .put("script_type", "lock");
            JSONArray params = new JSONArray()
                    .put(searchKey)
                    .put("asc")
                    .put("0x64");
            if (cursor != null) {
                params.put(cursor);
            }

            JSONObject response = postCkbRpc(new JSONObject()
                    .put("id", 1)
                    .put("jsonrpc", "2.0")
                    .put("method", "get_cells")
                    .put("params", params));
            if (response.has("error")) {
                throw new IOException(response.getJSONObject("error").optString("message", response.toString()));
            }
            JSONObject result = response.getJSONObject("result");
            JSONArray objects = result.optJSONArray("objects");
            if (objects != null) {
                for (int i = 0; i < objects.length(); i++) {
                    JSONObject output = objects.getJSONObject(i).getJSONObject("output");
                    total = total.add(hexQuantityToBigInteger(output.getString("capacity")));
                }
            }
            cursor = result.optString("last_cursor", null);
            if (objects == null || objects.length() == 0) {
                cursor = null;
            }
        } while (cursor != null);
        return total;
    }

    private static JSONObject postCkbRpc(JSONObject request) throws IOException, JSONException {
        HttpURLConnection connection = (HttpURLConnection) new URL(CKB_RPC_URL).openConnection();
        connection.setRequestMethod("POST");
        connection.setConnectTimeout(10_000);
        connection.setReadTimeout(20_000);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);
        byte[] body = request.toString().getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = connection.getOutputStream()) {
            output.write(body);
        }
        try (InputStream input = connection.getResponseCode() >= 400
                ? connection.getErrorStream()
                : connection.getInputStream()) {
            if (input == null) {
                throw new IOException("empty CKB RPC response");
            }
            byte[] buffer = new byte[8192];
            StringBuilder response = new StringBuilder();
            int read;
            while ((read = input.read(buffer)) != -1) {
                response.append(new String(buffer, 0, read, StandardCharsets.UTF_8));
            }
            return new JSONObject(response.toString());
        } finally {
            connection.disconnect();
        }
    }

    private static BigInteger hexQuantityToBigInteger(String value) {
        String hex = value.startsWith("0x") ? value.substring(2) : value;
        if (hex.isEmpty()) {
            return BigInteger.ZERO;
        }
        return new BigInteger(hex, 16);
    }

    private static String formatCkb(BigInteger shannons) {
        BigInteger[] parts = shannons.divideAndRemainder(SHANNONS_PER_CKB);
        BigInteger tenth = parts[1].multiply(BigInteger.TEN).divide(SHANNONS_PER_CKB);
        return parts[0] + "." + tenth + " CKB";
    }

    private static EcPoint multiply(EcPoint point, BigInteger scalar) {
        EcPoint result = EcPoint.INFINITY;
        EcPoint addend = point;
        for (int i = 0; i < scalar.bitLength(); i++) {
            if (scalar.testBit(i)) {
                result = add(result, addend);
            }
            addend = add(addend, addend);
        }
        return result;
    }

    private static EcPoint add(EcPoint p, EcPoint q) {
        if (p.infinity) {
            return q;
        }
        if (q.infinity) {
            return p;
        }
        if (p.x.equals(q.x)) {
            if (p.y.add(q.y).mod(FIELD_PRIME).signum() == 0) {
                return EcPoint.INFINITY;
            }
            BigInteger slope = p.x.pow(2).multiply(BigInteger.valueOf(3))
                    .multiply(p.y.shiftLeft(1).modInverse(FIELD_PRIME))
                    .mod(FIELD_PRIME);
            return pointFromSlope(slope, p, q);
        }
        BigInteger slope = q.y.subtract(p.y)
                .multiply(q.x.subtract(p.x).mod(FIELD_PRIME).modInverse(FIELD_PRIME))
                .mod(FIELD_PRIME);
        return pointFromSlope(slope, p, q);
    }

    private static EcPoint pointFromSlope(BigInteger slope, EcPoint p, EcPoint q) {
        BigInteger x = slope.pow(2).subtract(p.x).subtract(q.x).mod(FIELD_PRIME);
        BigInteger y = slope.multiply(p.x.subtract(x)).subtract(p.y).mod(FIELD_PRIME);
        return new EcPoint(x, y);
    }

    private static byte[] toFixedBytes(BigInteger value, int size) {
        byte[] source = value.toByteArray();
        byte[] output = new byte[size];
        int copyLength = Math.min(source.length, size);
        System.arraycopy(source, source.length - copyLength, output, size - copyLength, copyLength);
        return output;
    }

    private static byte[] blake2b(byte[] input, int outLength, byte[] personal) {
        byte[] param = new byte[64];
        param[0] = (byte) outLength;
        param[2] = 1;
        param[3] = 1;
        System.arraycopy(personal, 0, param, 48, Math.min(personal.length, 16));

        long[] h = BLAKE2B_IV.clone();
        for (int i = 0; i < h.length; i++) {
            h[i] ^= load64(param, i * 8);
        }
        byte[] block = new byte[128];
        System.arraycopy(input, 0, block, 0, input.length);
        compressBlake2b(h, block, input.length, true);

        byte[] output = new byte[outLength];
        for (int i = 0; i < outLength; i++) {
            output[i] = (byte) (h[i >>> 3] >>> (8 * (i & 7)));
        }
        return output;
    }

    private static void compressBlake2b(long[] h, byte[] block, long counter, boolean last) {
        long[] m = new long[16];
        long[] v = new long[16];
        for (int i = 0; i < m.length; i++) {
            m[i] = load64(block, i * 8);
        }
        System.arraycopy(h, 0, v, 0, 8);
        System.arraycopy(BLAKE2B_IV, 0, v, 8, 8);
        v[12] ^= counter;
        if (last) {
            v[14] = ~v[14];
        }
        for (int round = 0; round < 12; round++) {
            byte[] s = BLAKE2B_SIGMA[round];
            mix(v, 0, 4, 8, 12, m[s[0]], m[s[1]]);
            mix(v, 1, 5, 9, 13, m[s[2]], m[s[3]]);
            mix(v, 2, 6, 10, 14, m[s[4]], m[s[5]]);
            mix(v, 3, 7, 11, 15, m[s[6]], m[s[7]]);
            mix(v, 0, 5, 10, 15, m[s[8]], m[s[9]]);
            mix(v, 1, 6, 11, 12, m[s[10]], m[s[11]]);
            mix(v, 2, 7, 8, 13, m[s[12]], m[s[13]]);
            mix(v, 3, 4, 9, 14, m[s[14]], m[s[15]]);
        }
        for (int i = 0; i < 8; i++) {
            h[i] ^= v[i] ^ v[i + 8];
        }
    }

    private static void mix(long[] v, int a, int b, int c, int d, long x, long y) {
        v[a] = v[a] + v[b] + x;
        v[d] = Long.rotateRight(v[d] ^ v[a], 32);
        v[c] += v[d];
        v[b] = Long.rotateRight(v[b] ^ v[c], 24);
        v[a] = v[a] + v[b] + y;
        v[d] = Long.rotateRight(v[d] ^ v[a], 16);
        v[c] += v[d];
        v[b] = Long.rotateRight(v[b] ^ v[c], 63);
    }

    private static long load64(byte[] input, int offset) {
        long value = 0;
        for (int i = 7; i >= 0; i--) {
            value = (value << 8) | (input[offset + i] & 0xffL);
        }
        return value;
    }

    private static String bytesToHex(byte[] bytes) {
        char[] hex = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xff;
            hex[i * 2] = HEX[value >>> 4];
            hex[i * 2 + 1] = HEX[value & 0x0f];
        }
        return new String(hex);
    }

    private static String emptyToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private static String requireTrimmed(String value, String label) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(label + " is empty");
        }
        return value.trim();
    }

    private static String ckbAmountToShannonsHex(String value) {
        String trimmed = requireTrimmed(value, "amount");
        if (!trimmed.matches("[0-9]+(\\.[0-9]{1,8})?")) {
            throw new IllegalArgumentException("amount must be a decimal CKB amount with up to 8 fractional digits");
        }
        String[] parts = trimmed.split("\\.", -1);
        BigInteger whole = new BigInteger(parts[0], 10);
        BigInteger fractional = BigInteger.ZERO;
        if (parts.length == 2) {
            String paddedFraction = (parts[1] + "00000000").substring(0, 8);
            fractional = new BigInteger(paddedFraction, 10);
        }
        BigInteger amount = whole.multiply(SHANNONS_PER_CKB).add(fractional);
        if (amount.signum() <= 0) {
            throw new IllegalArgumentException("amount must be greater than 0");
        }
        if (amount.bitLength() > 128) {
            throw new IllegalArgumentException("amount exceeds u128");
        }
        return "0x" + amount.toString(16);
    }

    private static String shannonsAmountToHex(String value) {
        String trimmed = requireTrimmed(value, "amount");
        if (!trimmed.matches("[0-9]+")) {
            throw new IllegalArgumentException("amount must be an integer shannons amount");
        }
        BigInteger amount = new BigInteger(trimmed, 10);
        if (amount.signum() <= 0) {
            throw new IllegalArgumentException("amount must be greater than 0");
        }
        if (amount.bitLength() > 128) {
            throw new IllegalArgumentException("amount exceeds u128");
        }
        return "0x" + amount.toString(16);
    }

    private static void onNativeEvent(String eventJson) {
        for (NativeEventListener listener : nativeEventListeners) {
            listener.onNativeEvent(eventJson);
        }
    }

    private static synchronized void onCkbPrepared(int status, String resultJson) {
        boolean ready = false;
        boolean failed = status != 0;
        try {
            JSONObject result = new JSONObject(resultJson);
            ready = result.optBoolean("ready", false);
            failed = failed || "failed".equals(result.optString("status"));
        } catch (JSONException ignored) {
            failed = true;
        }
        if (ready || failed) {
            ckbPreparing = false;
            ckbReady = status == 0 && ready;
        }
        for (CkbPrepareListener listener : ckbPrepareListeners) {
            listener.onCkbPrepared(status, resultJson);
        }
    }

    private static NativeResult fromNativeCall(String value) {
        NativeResult result = NativeResult.fromPrefixed(value);
        if (!result.success && result.error != null && result.error.contains("node is not running")) {
            running = false;
        }
        return result;
    }

    private static String ensureNativeLibrariesLoaded() {
        if (nativeLibrariesLoaded) {
            return null;
        }
        if (nativeLoadError != null) {
            return nativeLoadError;
        }

        try {
            System.loadLibrary("fiber_ffi");
            System.loadLibrary("fiber_bridge");
            nativeLibrariesLoaded = true;
            return null;
        } catch (LinkageError error) {
            nativeLoadError = "Fiber start failed: cannot load native library: " + error.getMessage();
            return nativeLoadError;
        }
    }

    private static native String nativePrepareCkb(String configPath, String databasePrefix, String logLevel);

    private static native String nativeStart(String configPath, String databasePrefix, String logLevel);

    private static native String nativeStop();

    private static native String nativeNodeInfo();

    private static native String nativeListPeers();

    private static native String nativeConnectPeer(String address, String pubkey, String addrType, boolean save);

    private static native String nativeListChannels();

    private static native String nativeOpenChannel(String pubkey, String fundingAmountHex);

    private static native String nativeShutdownChannel(String channelId, boolean force);

    private static native String nativeNewInvoice(String amountHex, String description);

    private static native String nativeSendPayment(String invoice);

    public interface NativeEventListener {
        void onNativeEvent(String eventJson);
    }

    public interface CkbPrepareListener {
        void onCkbPrepared(int status, String resultJson);
    }

    public static final class CkbAccount {
        public final String pubkeyHash;

        private CkbAccount(String pubkeyHash) {
            this.pubkeyHash = pubkeyHash;
        }
    }

    public static final class CkbBalance {
        public final String pubkeyHash;
        public final BigInteger shannons;
        public final String formatted;

        private CkbBalance(String pubkeyHash, BigInteger shannons, String formatted) {
            this.pubkeyHash = pubkeyHash;
            this.shannons = shannons;
            this.formatted = formatted;
        }
    }

    private static final class EcPoint {
        private static final EcPoint INFINITY = new EcPoint(null, null, true);

        private final BigInteger x;
        private final BigInteger y;
        private final boolean infinity;

        private EcPoint(BigInteger x, BigInteger y) {
            this(x, y, false);
        }

        private EcPoint(BigInteger x, BigInteger y, boolean infinity) {
            this.x = x;
            this.y = y;
            this.infinity = infinity;
        }
    }

    public static final class NativeResult {
        public final boolean success;
        public final String value;
        public final String error;

        private NativeResult(boolean success, String value, String error) {
            this.success = success;
            this.value = value;
            this.error = error;
        }

        public static NativeResult error(String error) {
            return new NativeResult(false, null, error);
        }

        private static NativeResult fromPrefixed(String value) {
            if (value == null) {
                return error("Fiber call failed: empty native response");
            }
            if (value.startsWith("OK\n")) {
                return new NativeResult(true, value.substring(3), null);
            }
            if (value.startsWith("ERROR\n")) {
                return error(value.substring(6));
            }
            return error(value);
        }
    }
}
