package com.example.fiberdemo;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.res.Configuration;
import android.graphics.Typeface;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.text.InputType;
import android.text.TextUtils;
import android.view.Gravity;
import android.view.View;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainActivity extends AppCompatActivity {
    private static final BigInteger SHANNONS_PER_CKB = BigInteger.valueOf(100_000_000L);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Handler mainHandler = new Handler(Looper.getMainLooper());
    private final SimpleDateFormat logTimeFormat = new SimpleDateFormat("HH:mm:ss", Locale.US);
    private final FiberRuntime.NativeEventListener nativeEventListener =
            eventJson -> runOnUiThread(() -> appendLog("event: " + eventJson));

    private LinearLayout contentView;
    private TextView logView;
    private ScrollView logScrollView;
    private Button startStopButton;
    private Button ckbKeyButton;
    private Button nodeInfoButton;
    private Button peersButton;
    private Button invoiceButton;
    private Button channelsButton;
    private TextView invoiceResultView;
    private TextView addressView;
    private TextView pubkeyView;
    private String ckbBalanceLabel;
    private Page currentPage = Page.HOME;

    private enum Page {
        HOME,
        PEERS,
        INVOICE,
        CHANNELS
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        FiberRuntime.addNativeEventListener(nativeEventListener);

        int padding = getResources().getDimensionPixelSize(R.dimen.screen_padding);

        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setPadding(padding, padding, padding, padding);
        ViewCompat.setOnApplyWindowInsetsListener(root, (view, insets) -> {
            Insets safeInsets = insets.getInsets(
                    WindowInsetsCompat.Type.systemBars() | WindowInsetsCompat.Type.displayCutout()
            );
            view.setPadding(
                    padding + safeInsets.left,
                    padding + safeInsets.top,
                    padding + safeInsets.right,
                    padding + safeInsets.bottom
            );
            return insets;
        });

        contentView = new LinearLayout(this);
        contentView.setOrientation(LinearLayout.VERTICAL);

        logView = new TextView(this);
        logView.setTextSize(12);
        logView.setTypeface(Typeface.MONOSPACE);
        logView.setTextIsSelectable(true);
        logView.setPadding(padding / 2, padding / 2, padding / 2, padding / 2);

        logScrollView = new ScrollView(this);
        logScrollView.setFillViewport(true);
        applyLogColors();
        logScrollView.addView(logView, new ScrollView.LayoutParams(
                ScrollView.LayoutParams.MATCH_PARENT,
                ScrollView.LayoutParams.WRAP_CONTENT
        ));

        root.addView(contentView, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                3f
        ));
        root.addView(logScrollView, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                2f
        ));

        setContentView(root);
        showHome();
    }

    @Override
    protected void onDestroy() {
        FiberRuntime.removeNativeEventListener(nativeEventListener);
        executor.shutdownNow();
        super.onDestroy();
    }

    @Override
    protected void onResume() {
        super.onResume();
        if (FiberRuntime.isRunning()) {
            FiberNodeService.ensureStarted(this);
            if (currentPage == Page.HOME && addressView != null && pubkeyView != null) {
                refreshNodeInfo();
            }
        }
        updateHomeButtons();
    }

    @Override
    public void onBackPressed() {
        if (currentPage == Page.PEERS || currentPage == Page.INVOICE || currentPage == Page.CHANNELS) {
            showHome();
            return;
        }
        super.onBackPressed();
    }

    private void showHome() {
        currentPage = Page.HOME;
        contentView.removeAllViews();

        LinearLayout buttonRow = new LinearLayout(this);
        buttonRow.setOrientation(LinearLayout.HORIZONTAL);
        buttonRow.setGravity(Gravity.CENTER_VERTICAL);

        ckbKeyButton = new Button(this);
        ckbKeyButton.setAllCaps(false);
        ckbKeyButton.setOnClickListener(view -> handleCkbKeyButton());

        startStopButton = new Button(this);
        startStopButton.setAllCaps(false);
        startStopButton.setOnClickListener(view -> toggleNode());

        nodeInfoButton = new Button(this);
        nodeInfoButton.setText("NodeInfo");
        nodeInfoButton.setAllCaps(false);
        nodeInfoButton.setOnClickListener(view -> refreshNodeInfo());

        buttonRow.addView(ckbKeyButton, weightedWrapParams(1f));
        buttonRow.addView(startStopButton, weightedWrapParams(1f));
        buttonRow.addView(nodeInfoButton, weightedWrapParams(1f));
        contentView.addView(buttonRow, matchWrapParams());

        addressView = labelView("Address: ");
        pubkeyView = labelView("Pubkey: ");
        contentView.addView(addressView, matchWrapParams());
        contentView.addView(pubkeyView, matchWrapParams());

        LinearLayout navRow = new LinearLayout(this);
        navRow.setOrientation(LinearLayout.HORIZONTAL);
        navRow.setGravity(Gravity.TOP);

        LinearLayout leftNavColumn = new LinearLayout(this);
        leftNavColumn.setOrientation(LinearLayout.VERTICAL);

        LinearLayout rightNavColumn = new LinearLayout(this);
        rightNavColumn.setOrientation(LinearLayout.VERTICAL);

        peersButton = new Button(this);
        peersButton.setText("Peers");
        peersButton.setAllCaps(false);
        peersButton.setOnClickListener(view -> showPeers());

        invoiceButton = new Button(this);
        invoiceButton.setText("Invoice");
        invoiceButton.setAllCaps(false);
        invoiceButton.setOnClickListener(view -> showInvoice());

        channelsButton = new Button(this);
        channelsButton.setText("Channels");
        channelsButton.setAllCaps(false);
        channelsButton.setOnClickListener(view -> showChannels());

        leftNavColumn.addView(peersButton, matchWrapParams());
        leftNavColumn.addView(invoiceButton, matchWrapParams());
        rightNavColumn.addView(channelsButton, matchWrapParams());
        navRow.addView(leftNavColumn, weightedWrapParams(1f));
        navRow.addView(rightNavColumn, weightedWrapParams(1f));
        contentView.addView(navRow, matchWrapParams());

        View spacer = new View(this);
        contentView.addView(spacer, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1f
        ));

        updateHomeButtons();
        if (FiberRuntime.isRunning()) {
            refreshNodeInfo();
        }
    }

    private void showPeers() {
        currentPage = Page.PEERS;
        contentView.removeAllViews();

        LinearLayout topRow = new LinearLayout(this);
        topRow.setOrientation(LinearLayout.HORIZONTAL);
        topRow.setGravity(Gravity.CENTER_VERTICAL);

        Button backButton = new Button(this);
        backButton.setText("<");
        backButton.setAllCaps(false);
        backButton.setOnClickListener(view -> showHome());

        Button refreshButton = new Button(this);
        refreshButton.setText("ListPeer");
        refreshButton.setAllCaps(false);
        refreshButton.setOnClickListener(view -> refreshPeers());

        Button connectButton = new Button(this);
        connectButton.setText("Connect Peer");
        connectButton.setAllCaps(false);
        connectButton.setOnClickListener(view -> showConnectPeerDialog());

        topRow.addView(backButton, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 0.45f));
        topRow.addView(refreshButton, weightedWrapParams(1f));
        topRow.addView(connectButton, weightedWrapParams(1f));
        contentView.addView(topRow, matchWrapParams());

        TextView placeholder = labelView("No peers loaded");
        placeholder.setGravity(Gravity.CENTER);
        contentView.addView(placeholder, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1f
        ));

        refreshPeers();
    }

    private void showInvoice() {
        currentPage = Page.INVOICE;
        contentView.removeAllViews();

        LinearLayout topRow = new LinearLayout(this);
        topRow.setOrientation(LinearLayout.HORIZONTAL);
        topRow.setGravity(Gravity.CENTER_VERTICAL);

        Button backButton = new Button(this);
        backButton.setText("<");
        backButton.setAllCaps(false);
        backButton.setOnClickListener(view -> showHome());

        Button newInvoiceButton = new Button(this);
        newInvoiceButton.setText("New Invoice");
        newInvoiceButton.setAllCaps(false);
        newInvoiceButton.setOnClickListener(view -> showNewInvoiceDialog());

        Button sendPaymentButton = new Button(this);
        sendPaymentButton.setText("fiber_send_payment");
        sendPaymentButton.setAllCaps(false);
        sendPaymentButton.setOnClickListener(view -> showSendPaymentDialog());

        topRow.addView(backButton, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 0.45f));
        topRow.addView(newInvoiceButton, weightedWrapParams(1f));
        topRow.addView(sendPaymentButton, weightedWrapParams(1.25f));
        contentView.addView(topRow, matchWrapParams());

        invoiceResultView = labelView("No invoice generated");
        invoiceResultView.setGravity(Gravity.START);
        contentView.addView(invoiceResultView, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1f
        ));
    }

    private void showChannels() {
        currentPage = Page.CHANNELS;
        contentView.removeAllViews();

        LinearLayout topRow = new LinearLayout(this);
        topRow.setOrientation(LinearLayout.HORIZONTAL);
        topRow.setGravity(Gravity.CENTER_VERTICAL);

        Button backButton = new Button(this);
        backButton.setText("<");
        backButton.setAllCaps(false);
        backButton.setOnClickListener(view -> showHome());

        Button refreshButton = new Button(this);
        refreshButton.setText("ListChannels");
        refreshButton.setAllCaps(false);
        refreshButton.setOnClickListener(view -> refreshChannels());

        Button createButton = new Button(this);
        createButton.setText("CreateChannel");
        createButton.setAllCaps(false);
        createButton.setOnClickListener(view -> showCreateChannelDialog());

        topRow.addView(backButton, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 0.45f));
        topRow.addView(refreshButton, weightedWrapParams(1f));
        topRow.addView(createButton, weightedWrapParams(1f));
        contentView.addView(topRow, matchWrapParams());

        TextView placeholder = labelView("No channels loaded");
        placeholder.setGravity(Gravity.CENTER);
        contentView.addView(placeholder, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1f
        ));

        refreshChannels();
    }

    private void toggleNode() {
        if (!FiberRuntime.isRunning() && !FiberRuntime.hasCkbKey(this)) {
            appendLog("Start skipped: CKB private key is not set");
            showCkbKeyDialog();
            return;
        }
        setHomeBusy(true);
        if (FiberRuntime.isRunning()) {
            runFiberCall("stop", FiberRuntime::stop, result -> {
                FiberNodeService.ensureStopped(this);
                appendLog(result);
                updateHomeButtons();
            });
        } else {
            runFiberCall("start", () -> FiberRuntime.start(this), result -> {
                appendLog(result);
                updateHomeButtons();
                if (FiberRuntime.isRunning()) {
                    FiberNodeService.ensureStarted(this);
                    refreshNodeInfo();
                }
            });
        }
    }

    private void handleCkbKeyButton() {
        if (!FiberRuntime.hasCkbKey(this)) {
            showCkbKeyDialog();
            return;
        }
        refreshSavedCkbBalance();
    }

    private void showCkbKeyDialog() {
        LinearLayout form = new LinearLayout(this);
        form.setOrientation(LinearLayout.VERTICAL);
        int padding = getResources().getDimensionPixelSize(R.dimen.screen_padding);
        form.setPadding(padding, 0, padding, 0);

        EditText keyInput = new EditText(this);
        keyInput.setHint("Private Key");
        keyInput.setSingleLine(true);
        keyInput.setInputType(InputType.TYPE_CLASS_TEXT
                | InputType.TYPE_TEXT_VARIATION_VISIBLE_PASSWORD
                | InputType.TYPE_TEXT_FLAG_NO_SUGGESTIONS);
        form.addView(keyInput, matchWrapParams());

        LinearLayout balanceRow = new LinearLayout(this);
        balanceRow.setOrientation(LinearLayout.HORIZONTAL);
        balanceRow.setGravity(Gravity.CENTER_VERTICAL);

        TextView accountBalanceView = labelView("Account: \nBalance: ");
        Button refreshButton = new Button(this);
        refreshButton.setText("Refresh");
        refreshButton.setAllCaps(false);
        refreshButton.setOnClickListener(view -> refreshDialogCkbBalance(
                keyInput.getText().toString(),
                accountBalanceView,
                refreshButton
        ));

        balanceRow.addView(accountBalanceView, new LinearLayout.LayoutParams(
                0,
                LinearLayout.LayoutParams.WRAP_CONTENT,
                1f
        ));
        balanceRow.addView(refreshButton, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.WRAP_CONTENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
        ));
        form.addView(balanceRow, matchWrapParams());

        AlertDialog dialog = new AlertDialog.Builder(this)
                .setTitle("CKB Private Key")
                .setView(form)
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Confirm", null)
                .create();
        dialog.setOnShowListener(shownDialog -> dialog.getButton(DialogInterface.BUTTON_POSITIVE)
                .setOnClickListener(view -> confirmCkbPrivateKey(
                        dialog,
                        keyInput.getText().toString(),
                        accountBalanceView
                )));
        dialog.show();
    }

    private void confirmCkbPrivateKey(AlertDialog dialog, String privateKey, TextView statusView) {
        executor.execute(() -> {
            try {
                FiberRuntime.CkbAccount account = FiberRuntime.setCkbPrivateKey(this, privateKey);
                String balanceLabel = null;
                String balanceError = null;
                try {
                    balanceLabel = FiberRuntime.refreshCkbBalance(account.pubkeyHash).formatted;
                } catch (Exception exception) {
                    balanceError = exception.getMessage();
                }
                String finalBalanceLabel = balanceLabel;
                String finalBalanceError = balanceError;
                mainHandler.post(() -> {
                    ckbBalanceLabel = finalBalanceLabel;
                    appendLog("CKB key set: " + account.pubkeyHash);
                    if (finalBalanceError != null) {
                        appendLog("CKB balance refresh failed: " + finalBalanceError);
                    }
                    updateHomeButtons();
                    dialog.dismiss();
                });
            } catch (Exception exception) {
                mainHandler.post(() -> {
                    String message = "CKB key set failed: " + exception.getMessage();
                    statusView.setText(message);
                    appendLog(message);
                });
            }
        });
    }

    private void refreshDialogCkbBalance(String privateKey, TextView statusView, Button refreshButton) {
        refreshButton.setEnabled(false);
        statusView.setText("Refreshing...");
        executor.execute(() -> {
            try {
                FiberRuntime.CkbAccount account = FiberRuntime.previewCkbAccount(privateKey);
                FiberRuntime.CkbBalance balance = FiberRuntime.refreshCkbBalance(account.pubkeyHash);
                mainHandler.post(() -> {
                    statusView.setText("Account: " + account.pubkeyHash + "\nBalance: " + balance.formatted);
                    refreshButton.setEnabled(true);
                });
            } catch (Exception exception) {
                mainHandler.post(() -> {
                    statusView.setText("Error: " + exception.getMessage());
                    refreshButton.setEnabled(true);
                });
            }
        });
    }

    private void refreshSavedCkbBalance() {
        if (ckbKeyButton != null) {
            ckbKeyButton.setEnabled(false);
            ckbKeyButton.setText("Refreshing");
        }
        executor.execute(() -> {
            try {
                FiberRuntime.CkbBalance balance = FiberRuntime.refreshCkbBalance(this);
                mainHandler.post(() -> {
                    ckbBalanceLabel = balance.formatted;
                    appendLog("CKB balance: " + balance.formatted);
                    updateHomeButtons();
                });
            } catch (Exception exception) {
                mainHandler.post(() -> {
                    appendLog("CKB balance refresh failed: " + exception.getMessage());
                    updateHomeButtons();
                });
            }
        });
    }

    private void refreshNodeInfo() {
        if (!FiberRuntime.isRunning()) {
            appendLog("NodeInfo skipped: node is not running");
            return;
        }

        nodeInfoButton.setEnabled(false);
        executor.execute(() -> {
            FiberRuntime.NativeResult result = FiberRuntime.nodeInfo();
            mainHandler.post(() -> {
                nodeInfoButton.setEnabled(true);
                if (!result.success) {
                    appendLog(result.error);
                    return;
                }
                appendLog("NodeInfo refreshed");
                applyNodeInfo(result.value);
            });
        });
    }

    private void refreshPeers() {
        executor.execute(() -> {
            FiberRuntime.NativeResult result = FiberRuntime.listPeers();
            mainHandler.post(() -> {
                if (!result.success) {
                    appendLog(result.error);
                    setPeerListError(result.error);
                    return;
                }
                appendLog("ListPeer refreshed");
                applyPeerList(result.value);
            });
        });
    }

    private void refreshChannels() {
        executor.execute(() -> {
            FiberRuntime.NativeResult result = FiberRuntime.listChannels();
            mainHandler.post(() -> {
                if (!result.success) {
                    appendLog(result.error);
                    setChannelListError(result.error);
                    return;
                }
                appendLog("ListChannels refreshed");
                applyChannelList(result.value);
            });
        });
    }

    private void showConnectPeerDialog() {
        LinearLayout form = new LinearLayout(this);
        form.setOrientation(LinearLayout.VERTICAL);
        int padding = getResources().getDimensionPixelSize(R.dimen.screen_padding);
        form.setPadding(padding, 0, padding, 0);

        EditText addressInput = new EditText(this);
        addressInput.setHint("Address");
        form.addView(addressInput, matchWrapParams());

        EditText pubkeyInput = new EditText(this);
        pubkeyInput.setHint("Pubkey");
        form.addView(pubkeyInput, matchWrapParams());

        EditText addrTypeInput = new EditText(this);
        addrTypeInput.setHint("Addr Type: tcp, ws, or wss");
        form.addView(addrTypeInput, matchWrapParams());

        CheckBox saveInput = new CheckBox(this);
        saveInput.setText("Save peer address");
        form.addView(saveInput, matchWrapParams());

        new AlertDialog.Builder(this)
                .setTitle("Connect Peer")
                .setView(form)
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Connect", (dialog, which) -> connectPeer(
                        addressInput.getText().toString(),
                        pubkeyInput.getText().toString(),
                        addrTypeInput.getText().toString(),
                        saveInput.isChecked()
                ))
                .show();
    }

    private void connectPeer(String address, String pubkey, String addrType, boolean save) {
        executor.execute(() -> {
            FiberRuntime.NativeResult result = FiberRuntime.connectPeer(address, pubkey, addrType, save);
            mainHandler.post(() -> {
                if (!result.success) {
                    appendLog(result.error);
                    return;
                }
                appendLog(result.value);
                refreshPeers();
            });
        });
    }

    private void showNewInvoiceDialog() {
        LinearLayout form = new LinearLayout(this);
        form.setOrientation(LinearLayout.VERTICAL);
        int padding = getResources().getDimensionPixelSize(R.dimen.screen_padding);
        form.setPadding(padding, 0, padding, 0);

        EditText amountInput = new EditText(this);
        amountInput.setHint("Amount (shannons)");
        amountInput.setSingleLine(true);
        amountInput.setInputType(InputType.TYPE_CLASS_NUMBER);
        form.addView(amountInput, matchWrapParams());

        EditText descriptionInput = new EditText(this);
        descriptionInput.setHint("Description");
        descriptionInput.setSingleLine(true);
        form.addView(descriptionInput, matchWrapParams());

        new AlertDialog.Builder(this)
                .setTitle("New Invoice")
                .setView(form)
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Create", (dialog, which) -> newInvoice(
                        amountInput.getText().toString(),
                        descriptionInput.getText().toString()
                ))
                .show();
    }

    private void newInvoice(String amount, String description) {
        executor.execute(() -> {
            FiberRuntime.NativeResult result = FiberRuntime.newInvoice(amount, description);
            mainHandler.post(() -> {
                if (!result.success) {
                    appendLog(result.error);
                    setInvoiceResult(result.error);
                    return;
                }
                appendLog("New Invoice generated");
                setInvoiceResult(result.value);
            });
        });
    }

    private void showSendPaymentDialog() {
        LinearLayout form = new LinearLayout(this);
        form.setOrientation(LinearLayout.VERTICAL);
        int padding = getResources().getDimensionPixelSize(R.dimen.screen_padding);
        form.setPadding(padding, 0, padding, 0);

        EditText invoiceInput = new EditText(this);
        invoiceInput.setHint("Invoice");
        invoiceInput.setSingleLine(false);
        invoiceInput.setMinLines(3);
        form.addView(invoiceInput, matchWrapParams());

        new AlertDialog.Builder(this)
                .setTitle("fiber_send_payment")
                .setView(form)
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Send", (dialog, which) -> sendPayment(
                        invoiceInput.getText().toString()
                ))
                .show();
    }

    private void sendPayment(String invoice) {
        executor.execute(() -> {
            FiberRuntime.NativeResult result = FiberRuntime.sendPayment(invoice);
            mainHandler.post(() -> {
                if (!result.success) {
                    appendLog(result.error);
                    return;
                }
                appendLog(result.value);
            });
        });
    }

    private void showCreateChannelDialog() {
        LinearLayout form = new LinearLayout(this);
        form.setOrientation(LinearLayout.VERTICAL);
        int padding = getResources().getDimensionPixelSize(R.dimen.screen_padding);
        form.setPadding(padding, 0, padding, 0);

        EditText pubkeyInput = new EditText(this);
        pubkeyInput.setHint("PubKey");
        pubkeyInput.setSingleLine(true);
        form.addView(pubkeyInput, matchWrapParams());

        EditText amountInput = new EditText(this);
        amountInput.setHint("Amount (CKB)");
        amountInput.setSingleLine(true);
        amountInput.setInputType(InputType.TYPE_CLASS_NUMBER | InputType.TYPE_NUMBER_FLAG_DECIMAL);
        form.addView(amountInput, matchWrapParams());

        new AlertDialog.Builder(this)
                .setTitle("Create Channel")
                .setView(form)
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Create", (dialog, which) -> createChannel(
                        pubkeyInput.getText().toString(),
                        amountInput.getText().toString()
                ))
                .show();
    }

    private void createChannel(String pubkey, String amount) {
        executor.execute(() -> {
            FiberRuntime.NativeResult result = FiberRuntime.createChannel(pubkey, amount);
            mainHandler.post(() -> {
                if (!result.success) {
                    appendLog(result.error);
                    return;
                }
                appendLog(result.value);
                refreshChannels();
            });
        });
    }

    private void shutdownChannel(String channelId, Button button) {
        button.setEnabled(false);
        executor.execute(() -> {
            FiberRuntime.NativeResult result = FiberRuntime.shutdownChannel(channelId);
            mainHandler.post(() -> {
                if (!result.success) {
                    appendLog(result.error);
                    button.setEnabled(true);
                    return;
                }
                appendLog(result.value);
                refreshChannels();
            });
        });
    }

    private void applyNodeInfo(String json) {
        try {
            JSONObject object = new JSONObject(json);
            JSONArray addresses = object.optJSONArray("addresses");
            String address = "";
            if (addresses != null && addresses.length() > 0) {
                address = addresses.optString(0, "");
            }
            addressView.setText("Address: " + address);
            pubkeyView.setText("Pubkey: " + object.optString("pubkey", ""));
        } catch (JSONException exception) {
            appendLog("NodeInfo parse failed: " + exception.getMessage());
        }
    }

    private void applyPeerList(String json) {
        if (currentPage != Page.PEERS) {
            return;
        }

        removePeerContent();

        ScrollView scrollView = new ScrollView(this);
        LinearLayout list = new LinearLayout(this);
        list.setOrientation(LinearLayout.VERTICAL);
        scrollView.addView(list, new ScrollView.LayoutParams(
                ScrollView.LayoutParams.MATCH_PARENT,
                ScrollView.LayoutParams.WRAP_CONTENT
        ));

        try {
            JSONArray peers = new JSONObject(json).optJSONArray("peers");
            if (peers == null || peers.length() == 0) {
                TextView empty = labelView("No peers");
                empty.setGravity(Gravity.CENTER);
                list.addView(empty, matchWrapParams());
            } else {
                for (int i = 0; i < peers.length(); i++) {
                    JSONObject peer = peers.getJSONObject(i);
                    TextView row = labelView("Pubkey: " + peer.optString("pubkey", "")
                            + "\nAddress: " + peer.optString("address", ""));
                    row.setPadding(0, 12, 0, 12);
                    list.addView(row, matchWrapParams());
                }
            }
        } catch (JSONException exception) {
            TextView error = labelView("Peer list parse failed: " + exception.getMessage());
            list.addView(error, matchWrapParams());
            appendLog(error.getText().toString());
        }

        contentView.addView(scrollView, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1f
        ));
    }

    private void applyChannelList(String json) {
        if (currentPage != Page.CHANNELS) {
            return;
        }

        removePageContent();

        ScrollView scrollView = new ScrollView(this);
        LinearLayout list = new LinearLayout(this);
        list.setOrientation(LinearLayout.VERTICAL);
        scrollView.addView(list, new ScrollView.LayoutParams(
                ScrollView.LayoutParams.MATCH_PARENT,
                ScrollView.LayoutParams.WRAP_CONTENT
        ));

        try {
            JSONArray channels = new JSONObject(json).optJSONArray("channels");
            if (channels == null || channels.length() == 0) {
                TextView empty = labelView("No channels");
                empty.setGravity(Gravity.CENTER);
                list.addView(empty, matchWrapParams());
            } else {
                list.addView(channelRow("Channel ID", "Balance", "State Flags", null), matchWrapParams());
                for (int i = 0; i < channels.length(); i++) {
                    JSONObject channel = channels.getJSONObject(i);
                    String channelId = channel.optString("channel_id", "");
                    list.addView(channelRow(
                            channelId,
                            channelBalanceLabel(channel),
                            stateFlagsLabel(channel),
                            channelId
                    ), matchWrapParams());
                }
            }
        } catch (JSONException exception) {
            TextView error = labelView("Channel list parse failed: " + exception.getMessage());
            list.addView(error, matchWrapParams());
            appendLog(error.getText().toString());
        }

        contentView.addView(scrollView, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
            1f
        ));
    }

    private LinearLayout channelRow(String channelId, String balance, String stateFlags, String closeChannelId) {
        LinearLayout row = new LinearLayout(this);
        row.setOrientation(LinearLayout.HORIZONTAL);
        row.setGravity(Gravity.CENTER_VERTICAL);
        row.setPadding(0, 6, 0, 6);

        TextView channelIdView = labelView(channelId);
        channelIdView.setSingleLine(false);
        TextView balanceView = labelView(balance);
        balanceView.setSingleLine(false);
        TextView stateFlagsView = labelView(stateFlags);
        stateFlagsView.setSingleLine(false);

        row.addView(channelIdView, new LinearLayout.LayoutParams(
                0,
                LinearLayout.LayoutParams.WRAP_CONTENT,
                1.35f
        ));
        row.addView(balanceView, new LinearLayout.LayoutParams(
                0,
                LinearLayout.LayoutParams.WRAP_CONTENT,
                0.95f
        ));
        row.addView(stateFlagsView, new LinearLayout.LayoutParams(
                0,
                LinearLayout.LayoutParams.WRAP_CONTENT,
                0.85f
        ));

        if (closeChannelId == null) {
            TextView actionHeader = labelView("Action");
            row.addView(actionHeader, new LinearLayout.LayoutParams(
                    0,
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                    0.7f
            ));
        } else {
            Button closeButton = new Button(this);
            closeButton.setText("Close");
            closeButton.setAllCaps(false);
            closeButton.setEnabled(!closeChannelId.isEmpty());
            closeButton.setOnClickListener(view -> shutdownChannel(closeChannelId, closeButton));
            row.addView(closeButton, new LinearLayout.LayoutParams(
                    0,
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                    0.7f
            ));
        }

        return row;
    }

    private String channelBalanceLabel(JSONObject channel) {
        String localBalance = channel.optString("local_balance", "");
        if (localBalance.isEmpty()) {
            return "";
        }
        if (!channel.isNull("funding_udt_type_script")) {
            return localBalance;
        }
        try {
            return formatCkb(hexQuantityToBigInteger(localBalance));
        } catch (NumberFormatException exception) {
            return localBalance;
        }
    }

    private BigInteger hexQuantityToBigInteger(String value) {
        String hex = value.startsWith("0x") || value.startsWith("0X") ? value.substring(2) : value;
        if (hex.isEmpty()) {
            return BigInteger.ZERO;
        }
        return new BigInteger(hex, 16);
    }

    private String formatCkb(BigInteger shannons) {
        BigInteger[] parts = shannons.divideAndRemainder(SHANNONS_PER_CKB);
        BigInteger tenth = parts[1].multiply(BigInteger.TEN).divide(SHANNONS_PER_CKB);
        return parts[0] + "." + tenth + " CKB";
    }

    private String stateFlagsLabel(JSONObject channel) {
        JSONObject state = channel.optJSONObject("state");
        Object stateFlags = state == null ? channel.opt("state_flags") : state.opt("state_flags");
        if (stateFlags == null || JSONObject.NULL.equals(stateFlags)) {
            String stateName = state == null ? channel.optString("state_name", "") : state.optString("state_name", "");
            return stateName;
        }
        return stateFlags.toString();
    }

    private void setPeerListError(String errorMessage) {
        if (currentPage != Page.PEERS) {
            return;
        }
        removePageContent();
        TextView error = labelView(errorMessage);
        error.setGravity(Gravity.CENTER);
        contentView.addView(error, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1f
        ));
    }

    private void setChannelListError(String errorMessage) {
        if (currentPage != Page.CHANNELS) {
            return;
        }
        removePageContent();
        TextView error = labelView(errorMessage);
        error.setGravity(Gravity.CENTER);
        contentView.addView(error, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1f
        ));
    }

    private void runFiberCall(String action, FiberCall call, FiberResultConsumer consumer) {
        appendLog(action + " requested");
        executor.execute(() -> {
            String result = call.run();
            mainHandler.post(() -> {
                consumer.accept(result);
                setHomeBusy(false);
            });
        });
    }

    private void removePeerContent() {
        removePageContent();
    }

    private void removePageContent() {
        int childCount = contentView.getChildCount();
        if (childCount > 1) {
            contentView.removeViews(1, childCount - 1);
        }
    }

    private void setInvoiceResult(String text) {
        if (currentPage == Page.INVOICE && invoiceResultView != null) {
            invoiceResultView.setText(text);
        }
    }

    private void updateHomeButtons() {
        boolean running = FiberRuntime.isRunning();
        boolean hasCkbKey = FiberRuntime.hasCkbKey(this);
        if (ckbKeyButton != null) {
            ckbKeyButton.setText(hasCkbKey
                    ? (ckbBalanceLabel == null ? "Refresh CKB" : ckbBalanceLabel)
                    : "SetCKBKey");
            ckbKeyButton.setEnabled(true);
        }
        if (startStopButton != null) {
            startStopButton.setText(running ? R.string.fiber_stop : R.string.fiber_start);
            startStopButton.setEnabled(running || hasCkbKey);
        }
        if (nodeInfoButton != null) {
            nodeInfoButton.setEnabled(running);
        }
        if (peersButton != null) {
            peersButton.setEnabled(running);
        }
        if (invoiceButton != null) {
            invoiceButton.setEnabled(running);
        }
        if (channelsButton != null) {
            channelsButton.setEnabled(running);
        }
    }

    private void setHomeBusy(boolean busy) {
        if (ckbKeyButton != null) {
            ckbKeyButton.setEnabled(!busy);
        }
        if (startStopButton != null) {
            startStopButton.setEnabled(!busy && (FiberRuntime.isRunning() || FiberRuntime.hasCkbKey(this)));
        }
        if (nodeInfoButton != null) {
            nodeInfoButton.setEnabled(!busy && FiberRuntime.isRunning());
        }
        if (peersButton != null) {
            peersButton.setEnabled(!busy && FiberRuntime.isRunning());
        }
        if (invoiceButton != null) {
            invoiceButton.setEnabled(!busy && FiberRuntime.isRunning());
        }
        if (channelsButton != null) {
            channelsButton.setEnabled(!busy && FiberRuntime.isRunning());
        }
    }

    private void appendLog(String message) {
        if (TextUtils.isEmpty(message)) {
            return;
        }
        String line = logTimeFormat.format(new Date()) + "  " + message + "\n";
        logView.append(line);
        logScrollView.post(() -> logScrollView.fullScroll(View.FOCUS_DOWN));
    }

    private void applyLogColors() {
        if (isNightMode()) {
            logView.setTextColor(0xffffffff);
            logView.setBackgroundColor(0xff000000);
            logScrollView.setBackgroundColor(0xff000000);
            return;
        }
        logView.setTextColor(0xff000000);
        logView.setBackgroundColor(0xfff2f2f2);
        logScrollView.setBackgroundColor(0xfff2f2f2);
    }

    private boolean isNightMode() {
        int nightMode = getResources().getConfiguration().uiMode & Configuration.UI_MODE_NIGHT_MASK;
        return nightMode == Configuration.UI_MODE_NIGHT_YES;
    }

    private TextView labelView(String text) {
        TextView textView = new TextView(this);
        textView.setText(text);
        textView.setTextSize(14);
        textView.setTextIsSelectable(true);
        textView.setPadding(0, 8, 0, 8);
        return textView;
    }

    private LinearLayout.LayoutParams matchWrapParams() {
        return new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
        );
    }

    private LinearLayout.LayoutParams weightedWrapParams(float weight) {
        return new LinearLayout.LayoutParams(
                0,
                LinearLayout.LayoutParams.WRAP_CONTENT,
                weight
        );
    }

    private interface FiberCall {
        String run();
    }

    private interface FiberResultConsumer {
        void accept(String result);
    }
}
