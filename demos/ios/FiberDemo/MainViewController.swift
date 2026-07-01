import UIKit

final class MainViewController: UIViewController {
    private enum Page {
        case home
        case peers
        case invoice
        case channels
    }

    private let runtime = FiberRuntime.shared
    private let workQueue = DispatchQueue(label: "FiberDemo.WorkQueue")
    private let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()

    private let contentStack = UIStackView()
    private let logView = UITextView()
    private var currentPage = Page.home

    private weak var startStopButton: UIButton?
    private weak var ckbKeyButton: UIButton?
    private weak var nodeInfoButton: UIButton?
    private weak var peersButton: UIButton?
    private weak var invoiceButton: UIButton?
    private weak var channelsButton: UIButton?
    private weak var addressLabel: UILabel?
    private weak var pubkeyLabel: UILabel?
    private weak var invoiceResultLabel: UILabel?

    override func viewDidLoad() {
        super.viewDidLoad()
        title = "Fiber Demo"
        view.backgroundColor = .systemBackground
        configureLayout()
        runtime.setEventHandler { [weak self] eventJson in
            DispatchQueue.main.async {
                self?.appendLog("event: \(eventJson)")
            }
        }
        showHome()
    }

    deinit {
        runtime.setEventHandler(nil)
    }

    private func configureLayout() {
        let rootStack = UIStackView()
        rootStack.axis = .vertical
        rootStack.spacing = 12
        rootStack.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(rootStack)

        let contentScrollView = UIScrollView()
        contentScrollView.alwaysBounceVertical = true
        contentScrollView.translatesAutoresizingMaskIntoConstraints = false

        contentStack.axis = .vertical
        contentStack.spacing = 10
        contentStack.translatesAutoresizingMaskIntoConstraints = false
        contentScrollView.addSubview(contentStack)

        logView.isEditable = false
        logView.isSelectable = true
        logView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        logView.layer.cornerRadius = 6
        logView.backgroundColor = .secondarySystemBackground
        logView.textContainerInset = UIEdgeInsets(top: 8, left: 8, bottom: 8, right: 8)

        rootStack.addArrangedSubview(contentScrollView)
        rootStack.addArrangedSubview(logView)

        NSLayoutConstraint.activate([
            rootStack.leadingAnchor.constraint(equalTo: view.safeAreaLayoutGuide.leadingAnchor, constant: 16),
            rootStack.trailingAnchor.constraint(equalTo: view.safeAreaLayoutGuide.trailingAnchor, constant: -16),
            rootStack.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor, constant: 16),
            rootStack.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor, constant: -16),

            contentStack.leadingAnchor.constraint(equalTo: contentScrollView.contentLayoutGuide.leadingAnchor),
            contentStack.trailingAnchor.constraint(equalTo: contentScrollView.contentLayoutGuide.trailingAnchor),
            contentStack.topAnchor.constraint(equalTo: contentScrollView.contentLayoutGuide.topAnchor),
            contentStack.bottomAnchor.constraint(equalTo: contentScrollView.contentLayoutGuide.bottomAnchor),
            contentStack.widthAnchor.constraint(equalTo: contentScrollView.frameLayoutGuide.widthAnchor),

            contentScrollView.heightAnchor.constraint(equalTo: logView.heightAnchor, multiplier: 1.5)
        ])
    }

    private func showHome() {
        currentPage = .home
        resetContent()

        let keyButton = makeButton("SetCKBKey") { [weak self] in
            self?.handleCkbKeyButton()
        }
        let startButton = makeButton("Start") { [weak self] in
            self?.toggleNode()
        }
        let infoButton = makeButton("NodeInfo") { [weak self] in
            self?.refreshNodeInfo()
        }
        ckbKeyButton = keyButton
        startStopButton = startButton
        nodeInfoButton = infoButton
        contentStack.addArrangedSubview(buttonRow([keyButton, startButton, infoButton]))

        let address = makeLabel("Address: ")
        let pubkey = makeLabel("Pubkey: ")
        addressLabel = address
        pubkeyLabel = pubkey
        contentStack.addArrangedSubview(address)
        contentStack.addArrangedSubview(pubkey)

        let peers = makeButton("Peers") { [weak self] in
            self?.showPeers()
        }
        let invoice = makeButton("Invoice") { [weak self] in
            self?.showInvoice()
        }
        let channels = makeButton("Channels") { [weak self] in
            self?.showChannels()
        }
        peersButton = peers
        invoiceButton = invoice
        channelsButton = channels
        contentStack.addArrangedSubview(buttonRow([peers, invoice, channels]))

        updateHomeButtons()
        if runtime.isRunning {
            refreshNodeInfo()
        }
    }

    private func showPeers() {
        currentPage = .peers
        resetContent()
        contentStack.addArrangedSubview(buttonRow([
            makeButton("<") { [weak self] in self?.showHome() },
            makeButton("ListPeer") { [weak self] in self?.refreshPeers() },
            makeButton("Connect Peer") { [weak self] in self?.showConnectPeerDialog() }
        ]))
        contentStack.addArrangedSubview(centerLabel("No peers loaded"))
        refreshPeers()
    }

    private func showInvoice() {
        currentPage = .invoice
        resetContent()
        contentStack.addArrangedSubview(buttonRow([
            makeButton("<") { [weak self] in self?.showHome() },
            makeButton("New Invoice") { [weak self] in self?.showNewInvoiceDialog() },
            makeButton("fiber_send_payment") { [weak self] in self?.showSendPaymentDialog() }
        ]))

        let result = makeLabel("No invoice generated")
        result.numberOfLines = 0
        invoiceResultLabel = result
        contentStack.addArrangedSubview(result)
    }

    private func showChannels() {
        currentPage = .channels
        resetContent()
        contentStack.addArrangedSubview(buttonRow([
            makeButton("<") { [weak self] in self?.showHome() },
            makeButton("ListChannels") { [weak self] in self?.refreshChannels() },
            makeButton("CreateChannel") { [weak self] in self?.showCreateChannelDialog() }
        ]))
        contentStack.addArrangedSubview(centerLabel("No channels loaded"))
        refreshChannels()
    }

    private func toggleNode() {
        if !runtime.isRunning && !runtime.hasCkbKey() {
            appendLog("Start skipped: CKB private key is not set")
            showCkbKeyDialog()
            return
        }

        setHomeBusy(true)
        if runtime.isRunning {
            runFiberCall(action: "stop", call: { self.runtime.stop() }) { [weak self] result in
                self?.appendLog(result)
                self?.updateHomeButtons()
            }
        } else {
            runFiberCall(action: "start", call: { self.runtime.start() }) { [weak self] result in
                guard let self else {
                    return
                }
                appendLog(result)
                updateHomeButtons()
                if runtime.isRunning {
                    refreshNodeInfo()
                }
            }
        }
    }

    private func handleCkbKeyButton() {
        if runtime.hasCkbKey() {
            appendLog("CKB key is already set")
            return
        }
        showCkbKeyDialog()
    }

    private func showCkbKeyDialog() {
        let alert = UIAlertController(title: "CKB Private Key", message: nil, preferredStyle: .alert)
        alert.addTextField { textField in
            textField.placeholder = "Private Key"
            textField.autocapitalizationType = .none
            textField.autocorrectionType = .no
            textField.textContentType = .oneTimeCode
        }
        alert.addAction(UIAlertAction(title: "Cancel", style: .cancel))
        alert.addAction(UIAlertAction(title: "Confirm", style: .default) { [weak self, weak alert] _ in
            let privateKey = alert?.textFields?.first?.text ?? ""
            self?.confirmCkbPrivateKey(privateKey)
        })
        present(alert, animated: true)
    }

    private func confirmCkbPrivateKey(_ privateKey: String) {
        workQueue.async { [weak self] in
            do {
                try self?.runtime.setCkbPrivateKey(privateKey)
                DispatchQueue.main.async {
                    self?.appendLog("CKB key set")
                    self?.updateHomeButtons()
                }
            } catch {
                DispatchQueue.main.async {
                    self?.appendLog("CKB key set failed: \(error.localizedDescription)")
                }
            }
        }
    }

    private func refreshNodeInfo() {
        guard runtime.isRunning else {
            appendLog("NodeInfo skipped: node is not running")
            return
        }

        nodeInfoButton?.isEnabled = false
        workQueue.async { [weak self] in
            let result = self?.runtime.nodeInfo() ?? .failed("Fiber nodeInfo failed")
            DispatchQueue.main.async {
                self?.nodeInfoButton?.isEnabled = true
                guard result.success else {
                    self?.appendLog(result.error ?? "Fiber nodeInfo failed")
                    return
                }
                self?.appendLog("NodeInfo refreshed")
                self?.applyNodeInfo(result.value)
            }
        }
    }

    private func refreshPeers() {
        workQueue.async { [weak self] in
            let result = self?.runtime.listPeers() ?? .failed("Fiber listPeers failed")
            DispatchQueue.main.async {
                guard result.success else {
                    let error = result.error ?? "Fiber listPeers failed"
                    self?.appendLog(error)
                    self?.setPageError(error, page: .peers)
                    return
                }
                self?.appendLog("ListPeer refreshed")
                self?.applyPeerList(result.value)
            }
        }
    }

    private func refreshChannels() {
        workQueue.async { [weak self] in
            let result = self?.runtime.listChannels() ?? .failed("Fiber listChannels failed")
            DispatchQueue.main.async {
                guard result.success else {
                    let error = result.error ?? "Fiber listChannels failed"
                    self?.appendLog(error)
                    self?.setPageError(error, page: .channels)
                    return
                }
                self?.appendLog("ListChannels refreshed")
                self?.applyChannelList(result.value)
            }
        }
    }

    private func showConnectPeerDialog() {
        let alert = UIAlertController(title: "Connect Peer", message: nil, preferredStyle: .alert)
        alert.addTextField { $0.placeholder = "Address" }
        alert.addTextField { $0.placeholder = "Pubkey" }
        alert.addTextField { $0.placeholder = "Addr Type: tcp, ws, or wss" }
        alert.addAction(UIAlertAction(title: "Cancel", style: .cancel))
        alert.addAction(UIAlertAction(title: "Connect", style: .default) { [weak self, weak alert] _ in
            let fields = alert?.textFields ?? []
            self?.connectPeer(
                address: fields[safe: 0]?.text,
                pubkey: fields[safe: 1]?.text,
                addrType: fields[safe: 2]?.text,
                save: true
            )
        })
        present(alert, animated: true)
    }

    private func connectPeer(address: String?, pubkey: String?, addrType: String?, save: Bool) {
        workQueue.async { [weak self] in
            let result = self?.runtime.connectPeer(address: address, pubkey: pubkey, addrType: addrType, save: save)
                ?? .failed("Fiber connectPeer failed")
            DispatchQueue.main.async {
                guard result.success else {
                    self?.appendLog(result.error ?? "Fiber connectPeer failed")
                    return
                }
                self?.appendLog(result.value)
                self?.refreshPeers()
            }
        }
    }

    private func showNewInvoiceDialog() {
        let alert = UIAlertController(title: "New Invoice", message: nil, preferredStyle: .alert)
        alert.addTextField { textField in
            textField.placeholder = "Amount (shannons)"
            textField.keyboardType = .numberPad
        }
        alert.addTextField { $0.placeholder = "Description" }
        alert.addAction(UIAlertAction(title: "Cancel", style: .cancel))
        alert.addAction(UIAlertAction(title: "Create", style: .default) { [weak self, weak alert] _ in
            let fields = alert?.textFields ?? []
            self?.newInvoice(
                amount: fields[safe: 0]?.text ?? "",
                description: fields[safe: 1]?.text
            )
        })
        present(alert, animated: true)
    }

    private func newInvoice(amount: String, description: String?) {
        workQueue.async { [weak self] in
            let result = self?.runtime.newInvoice(amount: amount, description: description)
                ?? .failed("Fiber newInvoice failed")
            DispatchQueue.main.async {
                guard result.success else {
                    let error = result.error ?? "Fiber newInvoice failed"
                    self?.appendLog(error)
                    self?.invoiceResultLabel?.text = error
                    return
                }
                self?.appendLog("New Invoice generated")
                self?.invoiceResultLabel?.text = result.value
            }
        }
    }

    private func showSendPaymentDialog() {
        let alert = UIAlertController(title: "fiber_send_payment", message: nil, preferredStyle: .alert)
        alert.addTextField { textField in
            textField.placeholder = "Invoice"
            textField.autocapitalizationType = .none
            textField.autocorrectionType = .no
        }
        alert.addAction(UIAlertAction(title: "Cancel", style: .cancel))
        alert.addAction(UIAlertAction(title: "Send", style: .default) { [weak self, weak alert] _ in
            self?.sendPayment(invoice: alert?.textFields?.first?.text ?? "")
        })
        present(alert, animated: true)
    }

    private func sendPayment(invoice: String) {
        workQueue.async { [weak self] in
            let result = self?.runtime.sendPayment(invoice: invoice) ?? .failed("Fiber sendPayment failed")
            DispatchQueue.main.async {
                guard result.success else {
                    self?.appendLog(result.error ?? "Fiber sendPayment failed")
                    return
                }
                self?.appendLog(result.value)
            }
        }
    }

    private func showCreateChannelDialog() {
        let alert = UIAlertController(title: "Create Channel", message: nil, preferredStyle: .alert)
        alert.addTextField { textField in
            textField.placeholder = "PubKey"
            textField.autocapitalizationType = .none
            textField.autocorrectionType = .no
        }
        alert.addTextField { textField in
            textField.placeholder = "Amount (CKB)"
            textField.keyboardType = .decimalPad
        }
        alert.addAction(UIAlertAction(title: "Cancel", style: .cancel))
        alert.addAction(UIAlertAction(title: "Create", style: .default) { [weak self, weak alert] _ in
            let fields = alert?.textFields ?? []
            self?.createChannel(
                pubkey: fields[safe: 0]?.text ?? "",
                amount: fields[safe: 1]?.text ?? ""
            )
        })
        present(alert, animated: true)
    }

    private func createChannel(pubkey: String, amount: String) {
        workQueue.async { [weak self] in
            let result = self?.runtime.createChannel(pubkey: pubkey, amount: amount)
                ?? .failed("Fiber createChannel failed")
            DispatchQueue.main.async {
                guard result.success else {
                    self?.appendLog(result.error ?? "Fiber createChannel failed")
                    return
                }
                self?.appendLog(result.value)
                self?.refreshChannels()
            }
        }
    }

    private func shutdownChannel(_ channelId: String, button: UIButton) {
        button.isEnabled = false
        workQueue.async { [weak self] in
            let result = self?.runtime.shutdownChannel(channelId: channelId)
                ?? .failed("Fiber shutdownChannel failed")
            DispatchQueue.main.async {
                guard result.success else {
                    self?.appendLog(result.error ?? "Fiber shutdownChannel failed")
                    button.isEnabled = true
                    return
                }
                self?.appendLog(result.value)
                self?.refreshChannels()
            }
        }
    }

    private func applyNodeInfo(_ json: String) {
        do {
            let object = try jsonDictionary(json)
            let addresses = object["addresses"] as? [String]
            addressLabel?.text = "Address: \(addresses?.first ?? "")"
            pubkeyLabel?.text = "Pubkey: \((object["pubkey"] as? String) ?? "")"
        } catch {
            appendLog("NodeInfo parse failed: \(error.localizedDescription)")
        }
    }

    private func applyPeerList(_ json: String) {
        guard currentPage == .peers else {
            return
        }
        removePageContent()

        do {
            let object = try jsonDictionary(json)
            let peers = object["peers"] as? [[String: Any]] ?? []
            if peers.isEmpty {
                contentStack.addArrangedSubview(centerLabel("No peers"))
                return
            }

            for peer in peers {
                let pubkey = peer["pubkey"] as? String ?? ""
                let address = peer["address"] as? String ?? ""
                contentStack.addArrangedSubview(makeLabel("Pubkey: \(pubkey)\nAddress: \(address)"))
            }
        } catch {
            let message = "Peer list parse failed: \(error.localizedDescription)"
            appendLog(message)
            contentStack.addArrangedSubview(makeLabel(message))
        }
    }

    private func applyChannelList(_ json: String) {
        guard currentPage == .channels else {
            return
        }
        removePageContent()

        do {
            let object = try jsonDictionary(json)
            let channels = object["channels"] as? [[String: Any]] ?? []
            if channels.isEmpty {
                contentStack.addArrangedSubview(centerLabel("No channels"))
                return
            }

            contentStack.addArrangedSubview(channelRow(
                channelId: "Channel ID",
                balance: "Balance",
                state: "State Flags",
                closeChannelId: nil
            ))
            for channel in channels {
                let channelId = channel["channel_id"] as? String ?? ""
                contentStack.addArrangedSubview(channelRow(
                    channelId: channelId,
                    balance: channelBalanceLabel(channel),
                    state: stateFlagsLabel(channel),
                    closeChannelId: channelId
                ))
            }
        } catch {
            let message = "Channel list parse failed: \(error.localizedDescription)"
            appendLog(message)
            contentStack.addArrangedSubview(makeLabel(message))
        }
    }

    private func channelRow(channelId: String, balance: String, state: String, closeChannelId: String?) -> UIStackView {
        let row = UIStackView()
        row.axis = .horizontal
        row.alignment = .center
        row.spacing = 8

        let idLabel = makeLabel(channelId)
        let balanceLabel = makeLabel(balance)
        let stateLabel = makeLabel(state)
        idLabel.widthAnchor.constraint(equalTo: row.widthAnchor, multiplier: 0.34).isActive = true
        balanceLabel.widthAnchor.constraint(equalTo: row.widthAnchor, multiplier: 0.23).isActive = true
        stateLabel.widthAnchor.constraint(equalTo: row.widthAnchor, multiplier: 0.22).isActive = true
        row.addArrangedSubview(idLabel)
        row.addArrangedSubview(balanceLabel)
        row.addArrangedSubview(stateLabel)

        if let closeChannelId {
            let closeButton = makeButton("Close") { [weak self, weak row] in
                guard let button = row?.arrangedSubviews.last as? UIButton else {
                    return
                }
                self?.shutdownChannel(closeChannelId, button: button)
            }
            row.addArrangedSubview(closeButton)
        } else {
            row.addArrangedSubview(makeLabel("Action"))
        }
        return row
    }

    private func channelBalanceLabel(_ channel: [String: Any]) -> String {
        guard let localBalance = channel["local_balance"] as? String, !localBalance.isEmpty else {
            return ""
        }
        if let udt = channel["funding_udt_type_script"], !(udt is NSNull) {
            return localBalance
        }
        guard let shannons = hexQuantityToUInt64(localBalance) else {
            return localBalance
        }
        return formatCkb(shannons)
    }

    private func stateFlagsLabel(_ channel: [String: Any]) -> String {
        let state = channel["state"] as? [String: Any]
        let flags = state?["state_flags"] ?? channel["state_flags"]
        if let flags, !(flags is NSNull) {
            return String(describing: flags)
        }
        return (state?["state_name"] as? String) ?? (channel["state_name"] as? String) ?? ""
    }

    private func setPageError(_ message: String, page: Page) {
        guard currentPage == page else {
            return
        }
        removePageContent()
        contentStack.addArrangedSubview(centerLabel(message))
    }

    private func runFiberCall(action: String, call: @escaping () -> String, completion: @escaping (String) -> Void) {
        appendLog("\(action) requested")
        workQueue.async { [weak self] in
            let result = call()
            DispatchQueue.main.async {
                completion(result)
                self?.setHomeBusy(false)
            }
        }
    }

    private func updateHomeButtons() {
        let running = runtime.isRunning
        let hasKey = runtime.hasCkbKey()
        ckbKeyButton?.setTitle(hasKey ? "CKB Key Set" : "SetCKBKey", for: .normal)
        ckbKeyButton?.isEnabled = true
        startStopButton?.setTitle(running ? "Stop" : "Start", for: .normal)
        startStopButton?.isEnabled = running || hasKey
        nodeInfoButton?.isEnabled = running
        peersButton?.isEnabled = running
        invoiceButton?.isEnabled = running
        channelsButton?.isEnabled = running
    }

    private func setHomeBusy(_ busy: Bool) {
        let running = runtime.isRunning
        let hasKey = runtime.hasCkbKey()
        ckbKeyButton?.isEnabled = !busy
        startStopButton?.isEnabled = !busy && (running || hasKey)
        nodeInfoButton?.isEnabled = !busy && running
        peersButton?.isEnabled = !busy && running
        invoiceButton?.isEnabled = !busy && running
        channelsButton?.isEnabled = !busy && running
    }

    private func appendLog(_ message: String) {
        guard !message.isEmpty else {
            return
        }
        let line = "\(dateFormatter.string(from: Date()))  \(message)\n"
        logView.text.append(line)
        let bottom = NSRange(location: max(logView.text.count - 1, 0), length: 1)
        logView.scrollRangeToVisible(bottom)
    }

    private func resetContent() {
        for view in contentStack.arrangedSubviews {
            contentStack.removeArrangedSubview(view)
            view.removeFromSuperview()
        }
    }

    private func removePageContent() {
        let pageBody = contentStack.arrangedSubviews.dropFirst()
        for view in pageBody {
            contentStack.removeArrangedSubview(view)
            view.removeFromSuperview()
        }
    }

    private func makeButton(_ title: String, action: @escaping () -> Void) -> UIButton {
        let button = UIButton(type: .system)
        button.setTitle(title, for: .normal)
        button.titleLabel?.font = .systemFont(ofSize: 14, weight: .semibold)
        button.titleLabel?.numberOfLines = 2
        button.titleLabel?.textAlignment = .center
        button.configuration = .bordered()
        button.addAction(UIAction { _ in action() }, for: .touchUpInside)
        return button
    }

    private func makeLabel(_ text: String) -> UILabel {
        let label = UILabel()
        label.text = text
        label.font = .systemFont(ofSize: 14)
        label.numberOfLines = 0
        label.lineBreakMode = .byCharWrapping
        return label
    }

    private func centerLabel(_ text: String) -> UILabel {
        let label = makeLabel(text)
        label.textAlignment = .center
        return label
    }

    private func buttonRow(_ buttons: [UIButton]) -> UIStackView {
        let row = UIStackView(arrangedSubviews: buttons)
        row.axis = .horizontal
        row.spacing = 8
        row.distribution = .fillEqually
        return row
    }
}

private func jsonDictionary(_ json: String) throws -> [String: Any] {
    let data = Data(json.utf8)
    let object = try JSONSerialization.jsonObject(with: data)
    guard let dictionary = object as? [String: Any] else {
        throw JsonError.notDictionary
    }
    return dictionary
}

private func hexQuantityToUInt64(_ value: String) -> UInt64? {
    var hex = value
    if hex.hasPrefix("0x") || hex.hasPrefix("0X") {
        hex.removeFirst(2)
    }
    return UInt64(hex, radix: 16)
}

private func formatCkb(_ shannons: UInt64) -> String {
    let whole = shannons / 100_000_000
    let fractionalTenth = (shannons % 100_000_000) * 10 / 100_000_000
    return "\(whole).\(fractionalTenth) CKB"
}

private enum JsonError: LocalizedError {
    case notDictionary

    var errorDescription: String? {
        "JSON root is not an object"
    }
}

private extension Collection {
    subscript(safe index: Index) -> Element? {
        indices.contains(index) ? self[index] : nil
    }
}
