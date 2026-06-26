import Foundation
import Darwin

private let fiberStatusOk = FiberFfiStatus(0)

private let fiberEventCallback: fiber_event_callback = { eventJson, _ in
    guard let eventJson else {
        return
    }
    FiberRuntime.shared.emitNativeEvent(String(cString: eventJson))
}

final class FiberRuntime {
    static let shared = FiberRuntime()

    struct NativeResult {
        let success: Bool
        let value: String
        let error: String?

        static func ok(_ value: String) -> NativeResult {
            NativeResult(success: true, value: value, error: nil)
        }

        static func failed(_ error: String) -> NativeResult {
            NativeResult(success: false, value: "", error: error)
        }
    }

    private let lock = NSRecursiveLock()
    private let fileManager = FileManager.default
    private var handle: OpaquePointer?
    private var runningFlag = false
    private var eventHandler: ((String) -> Void)?

    private init() {}

    var isRunning: Bool {
        lock.withLock {
            runningFlag && handle != nil
        }
    }

    func setEventHandler(_ handler: ((String) -> Void)?) {
        lock.withLock {
            eventHandler = handler
        }
    }

    func start() -> String {
        lock.lock()
        defer { lock.unlock() }

        if handle != nil {
            return "Fiber already running"
        }

        guard hasCkbKeyLocked() else {
            return "Fiber start failed: CKB private key is not set"
        }

        do {
            let configURL = try ensureConfigFile()
            let dataURL = try ensureDataDirectory()
            setenv("FIBER_SECRET_KEY_PASSWORD", "fiber-demo-secret-key-password", 0)

            var newHandle: OpaquePointer?
            let status = configURL.path.withCString { configPath in
                dataURL.path.withCString { databasePrefix in
                    "info".withCString { logLevel in
                        var options = FiberStartOptions()
                        options.config_path = configPath
                        options.database_prefix = databasePrefix
                        options.log_level = logLevel
                        options.event_callback = fiberEventCallback
                        options.event_callback_user_data = nil
                        return fiber_start(&options, &newHandle)
                    }
                }
            }

            if status == fiberStatusOk {
                handle = newHandle
                runningFlag = true
            }
            return resultMessage(action: "started", status: status)
        } catch {
            return "Fiber start failed: \(error.localizedDescription)"
        }
    }

    func stop() -> String {
        lock.lock()
        defer { lock.unlock() }

        guard let currentHandle = handle else {
            runningFlag = false
            return "Fiber already stopped"
        }

        handle = nil
        runningFlag = false
        let status = fiber_stop(currentHandle)
        return resultMessage(action: "stopped", status: status)
    }

    func stopIfRunning() {
        if isRunning {
            _ = stop()
        }
    }

    func nodeInfo() -> NativeResult {
        withHandle(action: "nodeInfo") { handle in
            var json: UnsafeMutablePointer<CChar>?
            let status = fiber_node_info(handle, &json)
            return jsonResult(status: status, json: json, action: "nodeInfo")
        }
    }

    func listPeers() -> NativeResult {
        withHandle(action: "listPeers") { handle in
            var json: UnsafeMutablePointer<CChar>?
            let status = fiber_list_peers(handle, &json)
            return jsonResult(status: status, json: json, action: "listPeers")
        }
    }

    func connectPeer(address: String?, pubkey: String?, addrType: String?, save: Bool) -> NativeResult {
        withHandle(action: "connectPeer") { handle in
            withOptionalCString(address) { addressPtr in
                withOptionalCString(pubkey) { pubkeyPtr in
                    withOptionalCString(addrType) { addrTypePtr in
                        var options = FiberConnectPeerOptions()
                        options.address = addressPtr
                        options.pubkey = pubkeyPtr
                        options.addr_type = addrTypePtr
                        options.save = save ? 1 : 0

                        let status = fiber_connect_peer(handle, &options)
                        if status == fiberStatusOk {
                            return NativeResult.ok("Fiber peer connected")
                        }
                        return NativeResult.failed(resultMessage(action: "connectPeer", status: status))
                    }
                }
            }
        }
    }

    func listChannels() -> NativeResult {
        withHandle(action: "listChannels") { handle in
            var options = FiberListChannelsOptions()
            options.struct_size = UInt32(MemoryLayout<FiberListChannelsOptions>.size)
            options.flags = 0

            var json: UnsafeMutablePointer<CChar>?
            let status = fiber_list_channels(handle, &options, &json)
            return jsonResult(status: status, json: json, action: "listChannels")
        }
    }

    func createChannel(pubkey: String, amount: String) -> NativeResult {
        withHandle(action: "createChannel") { handle in
            do {
                let fundingAmount = try parseCkbAmount(amount)
                return pubkey.trimmingCharacters(in: .whitespacesAndNewlines).withCString { pubkeyPtr in
                    var options = FiberOpenChannelOptions()
                    options.struct_size = UInt32(MemoryLayout<FiberOpenChannelOptions>.size)
                    options.flags = 0
                    options.pubkey = pubkeyPtr
                    options.funding_amount = fundingAmount.ffi

                    var temporaryChannelId: UnsafeMutablePointer<CChar>?
                    let status = fiber_open_channel(handle, &options, &temporaryChannelId)
                    return jsonResult(status: status, json: temporaryChannelId, action: "createChannel")
                }
            } catch {
                return .failed("Fiber createChannel failed: \(error.localizedDescription)")
            }
        }
    }

    func shutdownChannel(channelId: String) -> NativeResult {
        withHandle(action: "shutdownChannel") { handle in
            do {
                let trimmed = try requireTrimmed(channelId, label: "channel_id")
                return trimmed.withCString { channelIdPtr in
                    var options = FiberShutdownChannelOptions()
                    options.struct_size = UInt32(MemoryLayout<FiberShutdownChannelOptions>.size)
                    options.flags = 0
                    options.channel_id = channelIdPtr
                    options.has_force = 1
                    options.force = 1

                    let status = fiber_shutdown_channel(handle, &options)
                    if status == fiberStatusOk {
                        return NativeResult.ok("Fiber channel shutdown requested")
                    }
                    return NativeResult.failed(resultMessage(action: "shutdownChannel", status: status))
                }
            } catch {
                return .failed("Fiber shutdownChannel failed: \(error.localizedDescription)")
            }
        }
    }

    func newInvoice(amount: String, description: String?) -> NativeResult {
        withHandle(action: "newInvoice") { handle in
            do {
                let invoiceAmount = try parseShannonsAmount(amount)
                return withOptionalCString(description) { descriptionPtr in
                    var options = FiberNewInvoiceOptions()
                    options.struct_size = UInt32(MemoryLayout<FiberNewInvoiceOptions>.size)
                    options.flags = 0
                    options.amount = invoiceAmount.ffi
                    options.description = descriptionPtr
                    options.currency = FiberInvoiceCurrency(3)

                    var invoiceAddress: UnsafeMutablePointer<CChar>?
                    let status = fiber_new_invoice(handle, &options, &invoiceAddress)
                    return jsonResult(status: status, json: invoiceAddress, action: "newInvoice")
                }
            } catch {
                return .failed("Fiber newInvoice failed: \(error.localizedDescription)")
            }
        }
    }

    func sendPayment(invoice: String) -> NativeResult {
        withHandle(action: "sendPayment") { handle in
            do {
                let trimmed = try requireTrimmed(invoice, label: "invoice")
                return trimmed.withCString { invoicePtr in
                    var options = FiberSendPaymentOptions()
                    options.struct_size = UInt32(MemoryLayout<FiberSendPaymentOptions>.size)
                    options.flags = 0
                    options.invoice = invoicePtr

                    var json: UnsafeMutablePointer<CChar>?
                    let status = fiber_send_payment(handle, &options, &json)
                    return jsonResult(status: status, json: json, action: "sendPayment")
                }
            } catch {
                return .failed("Fiber sendPayment failed: \(error.localizedDescription)")
            }
        }
    }

    func hasCkbKey() -> Bool {
        lock.withLock {
            hasCkbKeyLocked()
        }
    }

    func setCkbPrivateKey(_ privateKeyHex: String) throws {
        let normalized = try normalizePrivateKey(privateKeyHex)
        let keyURL = try ckbKeyURL()
        try fileManager.createDirectory(
            at: keyURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try "\(normalized)\n".write(to: keyURL, atomically: true, encoding: .ascii)
    }

    fileprivate func emitNativeEvent(_ eventJson: String) {
        let handler = lock.withLock {
            eventHandler
        }
        handler?(eventJson)
    }

    private func withHandle(action: String, _ body: (OpaquePointer) -> NativeResult) -> NativeResult {
        lock.lock()
        defer { lock.unlock() }

        guard let handle else {
            runningFlag = false
            return .failed("Fiber \(action) failed: node is not running")
        }
        return body(handle)
    }

    private func jsonResult(status: FiberFfiStatus, json: UnsafeMutablePointer<CChar>?, action: String) -> NativeResult {
        if status == fiberStatusOk {
            if let json {
                let value = String(cString: json)
                fiber_string_free(json)
                return .ok(value)
            }
            return .ok("")
        }
        if let json {
            fiber_string_free(json)
        }
        return .failed(resultMessage(action: action, status: status))
    }

    private func resultMessage(action: String, status: FiberFfiStatus) -> String {
        if status == fiberStatusOk {
            return "Fiber \(action)"
        }

        var message = "Fiber \(action) failed (\(status))"
        let lastError = lastErrorMessage()
        if !lastError.isEmpty {
            message += ": \(lastError)"
        }
        return message
    }

    private func lastErrorMessage() -> String {
        let required = fiber_last_error_message(nil, 0)
        if required == 0 {
            return ""
        }

        var buffer = [CChar](repeating: 0, count: required + 1)
        let written = fiber_last_error_message(&buffer, buffer.count)
        if written == 0 {
            return ""
        }
        return String(cString: buffer)
    }

    private func ensureConfigFile() throws -> URL {
        guard let bundledConfig = Bundle.main.url(forResource: "fiber_config", withExtension: "yml") else {
            throw RuntimeError.message("fiber_config.yml is missing from the app bundle")
        }

        let destination = documentsURL().appendingPathComponent("fiber_config.yml")
        if fileManager.fileExists(atPath: destination.path) {
            try fileManager.removeItem(at: destination)
        }
        try fileManager.copyItem(at: bundledConfig, to: destination)
        return destination
    }

    private func ensureDataDirectory() throws -> URL {
        let url = documentsURL().appendingPathComponent("fiber-data", isDirectory: true)
        try fileManager.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    private func ckbKeyURL() throws -> URL {
        try ensureDataDirectory()
            .appendingPathComponent("ckb", isDirectory: true)
            .appendingPathComponent("key")
    }

    private func hasCkbKeyLocked() -> Bool {
        guard let keyURL = try? ckbKeyURL() else {
            return false
        }
        return fileManager.fileExists(atPath: keyURL.path)
    }

    private func documentsURL() -> URL {
        fileManager.urls(for: .documentDirectory, in: .userDomainMask)[0]
    }
}

private func withOptionalCString<T>(_ value: String?, _ body: (UnsafePointer<CChar>?) -> T) -> T {
    guard let trimmed = value?.trimmingCharacters(in: .whitespacesAndNewlines), !trimmed.isEmpty else {
        return body(nil)
    }
    return trimmed.withCString { pointer in
        body(pointer)
    }
}

private func requireTrimmed(_ value: String, label: String) throws -> String {
    let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
    if trimmed.isEmpty {
        throw RuntimeError.message("\(label) is empty")
    }
    return trimmed
}

private func normalizePrivateKey(_ privateKeyHex: String) throws -> String {
    var value = privateKeyHex.trimmingCharacters(in: .whitespacesAndNewlines)
    if value.hasPrefix("0x") || value.hasPrefix("0X") {
        value.removeFirst(2)
    }
    value = value.lowercased()

    guard value.count == 64 else {
        throw RuntimeError.message("private key must be 32 bytes hex")
    }
    guard value.allSatisfy({ $0.isHexDigit }) else {
        throw RuntimeError.message("private key must be hex")
    }
    guard value.contains(where: { $0 != "0" }) else {
        throw RuntimeError.message("private key is outside secp256k1 range")
    }

    let secp256k1Order = "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141"
    if value >= secp256k1Order {
        throw RuntimeError.message("private key is outside secp256k1 range")
    }
    return value
}

private func parseCkbAmount(_ text: String) throws -> UInt128Value {
    let trimmed = try requireTrimmed(text, label: "amount")
    let parts = trimmed.split(separator: ".", omittingEmptySubsequences: false)
    guard parts.count == 1 || parts.count == 2, !parts[0].isEmpty else {
        throw RuntimeError.message("amount must be a decimal CKB amount with up to 8 fractional digits")
    }

    var whole = try parseDecimalDigits(String(parts[0]))
    try whole.multiply(by: 100_000_000)

    if parts.count == 2 {
        let fraction = String(parts[1])
        guard !fraction.isEmpty && fraction.count <= 8 else {
            throw RuntimeError.message("amount must be a decimal CKB amount with up to 8 fractional digits")
        }
        let padded = fraction + String(repeating: "0", count: 8 - fraction.count)
        let fractional = try parseDecimalDigits(padded)
        try whole.add(fractional)
    }

    if whole.isZero {
        throw RuntimeError.message("amount must be greater than 0")
    }
    return whole
}

private func parseShannonsAmount(_ text: String) throws -> UInt128Value {
    let trimmed = try requireTrimmed(text, label: "amount")
    let amount = try parseDecimalDigits(trimmed)
    if amount.isZero {
        throw RuntimeError.message("amount must be greater than 0")
    }
    return amount
}

private func parseDecimalDigits(_ text: String) throws -> UInt128Value {
    guard !text.isEmpty else {
        throw RuntimeError.message("amount must be a decimal integer")
    }

    var value = UInt128Value()
    for scalar in text.unicodeScalars {
        guard scalar.value >= 48 && scalar.value <= 57 else {
            throw RuntimeError.message("amount must be a decimal integer")
        }
        try value.multiply(by: 10)
        try value.add(UInt64(scalar.value - 48))
    }
    return value
}

private struct UInt128Value {
    var low: UInt64 = 0
    var high: UInt64 = 0

    var isZero: Bool {
        low == 0 && high == 0
    }

    var ffi: FiberU128 {
        FiberU128(low: low, high: high)
    }

    mutating func multiply(by factor: UInt64) throws {
        let lowProduct = low.multipliedFullWidth(by: factor)
        let highProduct = high.multipliedFullWidth(by: factor)
        let (newHigh, highOverflow) = highProduct.low.addingReportingOverflow(lowProduct.high)
        if highProduct.high != 0 || highOverflow {
            throw RuntimeError.message("amount exceeds u128")
        }
        low = lowProduct.low
        high = newHigh
    }

    mutating func add(_ value: UInt64) throws {
        let (newLow, carry) = low.addingReportingOverflow(value)
        let (newHigh, overflow) = high.addingReportingOverflow(carry ? 1 : 0)
        if overflow {
            throw RuntimeError.message("amount exceeds u128")
        }
        low = newLow
        high = newHigh
    }

    mutating func add(_ value: UInt128Value) throws {
        let (newLow, carry) = low.addingReportingOverflow(value.low)
        let (partialHigh, highOverflow) = high.addingReportingOverflow(value.high)
        let (newHigh, carryOverflow) = partialHigh.addingReportingOverflow(carry ? 1 : 0)
        if highOverflow || carryOverflow {
            throw RuntimeError.message("amount exceeds u128")
        }
        low = newLow
        high = newHigh
    }
}

private enum RuntimeError: LocalizedError {
    case message(String)

    var errorDescription: String? {
        switch self {
        case let .message(message):
            return message
        }
    }
}

private extension NSRecursiveLock {
    func withLock<T>(_ body: () -> T) -> T {
        lock()
        defer { unlock() }
        return body()
    }
}
