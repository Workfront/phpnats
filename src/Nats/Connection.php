<?php

namespace Nats;

/**
 * Connection Class.
 *
 * Handles the connection to a NATS server or cluster of servers.
 *
 * @package Nats
 */
class Connection
{

    /**
     * Show DEBUG info?
     *
     * @var boolean $debug If debug is enabled.
     */
    private $debug = false;

    /**
     * Number of PINGs.
     *
     * @var integer number of pings.
     */
    private $pings = 0;

    /**
     * Chunk size in bytes to use when reading an stream of data.
     *
     * @var integer size of chunk.
     */
    private $chunkSize = 1500;

    /**
     * Number of messages published.
     *
     * @var int number of messages
     */
    private $pubs = 0;

    /**
     * Number of reconnects to the server.
     *
     * @var int Number of reconnects
     */
    private $reconnects = 0;
    
    /**
     * List of available subscriptions.
     *
     * @var array list of subscriptions
     */
    private $subscriptions = [];
    
    /**
     * List of registered subscriptions.
     *
     * @var array list of registered subscriptions
     */
    private $registeredSubscriptions = [];

    /**
     * Connection options object.
     *
     * @var ConnectionOptions|null
     */
    private $options = null;

    /**
     * Connection timeout
     *
     * @var float
     */
    private $timeout = null;

    /**
     * Stream File Pointer.
     *
     * @var mixed Socket file pointer
     */
    private $streamSocket;
    
    /**
     * Server information.
     *
     * @var mixed
     */
    private $serverInfo;
    
    /**
     * Enable or disable debug mode.
     *
     * @param boolean $debug If debug is enabled.
     *
     * @return void
     */
    public function setDebug($debug)
    {
        $this->debug = $debug;
    }
    
    /**
     * Return the number of pings.
     *
     * @return integer Number of pings
     */
    public function pingsCount()
    {
        return $this->pings;
    }
    
    /**
     * Return the number of messages published.
     *
     * @return integer number of messages published
     */
    public function pubsCount()
    {
        return $this->pubs;
    }
    
    /**
     * Return the number of reconnects to the server.
     *
     * @return integer number of reconnects
     */
    public function reconnectsCount()
    {
        return $this->reconnects;
    }
    
    /**
     * Return the number of subscriptions available.
     *
     * @return integer number of subscription
     */
    public function subscriptionsCount()
    {
        return count($this->subscriptions);
    }
    
    /**
     * Return subscriptions list.
     *
     * @return array list of subscription ids
     */
    public function getSubscriptions()
    {
        return array_keys($this->subscriptions);
    }

    /**
     * Sets the chunck size in bytes to be processed when reading.
     *
     * @param integer $chunkSize Set byte chunk len to read when reading from wire.
     *
     * @return void
     */
    public function setChunkSize($chunkSize)
    {
        $this->chunkSize = $chunkSize;
    }

    /**
     * Set Stream Timeout.
     *
     * @author ikubicki
     * @param float $timeout Before timeout on stream.
     * @return boolean
     */
    public function setStreamTimeout($timeout)
    {
        if (!is_numeric($timeout)) {
            return false;
        }
        $this->timeout = $timeout;
        if (!$this->isConnected()) {
            return false;
        }
        list($number, $decimals) = $this->getNumberAndDecimals($timeout);
        return stream_set_timeout($this->streamSocket, $number, $decimals);
    }

    /**
     * Returns an stream socket for this connection.
     *
     * @return resource
     */
    public function getStreamSocket()
    {
        return $this->streamSocket;
    }

    /**
     * Indicates whether $response is an error response.
     *
     * @param string $response The Nats Server response.
     * @return boolean
     */
    private function isErrorResponse($response)
    {
        return substr($response, 0, 4) === '-ERR';
    }


    /**
     * Checks if the client is connected to a server.
     *
     * @return boolean
     */
    public function isConnected()
    {
        return isset($this->streamSocket);
    }

    /**
     * Returns an stream socket to the desired server.
     *
     * @param string $address Server url string.
     * @param float $timeout Number of seconds until the connect() system call should timeout.
     * @throws \Exception Exception raised if connection fails.
     * @return resource
     */
    private function getStream($address, $timeout, $context)
    {
        $errno  = null;
        $errstr = null;

        set_error_handler(function () {return true;});
        $fp = stream_socket_client($address, $errno, $errstr, $timeout, STREAM_CLIENT_CONNECT, $context);
        restore_error_handler();

        if ($fp === false) {
            throw Exception::forStreamSocketClientError($errstr, $errno);
        }
        $this->setStreamTimeout($timeout);
        return $fp;
    }


    /**
     * Process information returned by the server after connection.
     *
     * @param string $connectionResponse INFO message.
     *
     * @return void
     */
    private function processServerInfo($connectionResponse)
    {
        $this->serverInfo = new ServerInfo($connectionResponse);
    }

    /**
     * Returns current connected server ID.
     *
     * @return string Server ID.
     */
    public function connectedServerID()
    {
        return $this->serverInfo->getServerID();
    }

    /**
     * Constructor.
     *
     * @param ConnectionOptions $options Connection options object.
     */
    public function __construct(ConnectionOptions $options = null)
    {
        $this->pings = 0;
        $this->pubs = 0;
        $this->subscriptions = [];
        $this->options = $options;

        if ($options === null) {
            $this->options = new ConnectionOptions();
        }
    }

    /**
     * Sends data thought the stream.
     *
     * @param string $payload Message data.
     *
     * @throws \Exception Raises if fails sending data.
     * @return void
     */
    private function send($payload)
    {
        $msg = $payload."\r\n";
        $len = strlen($msg);
        while (true) {
            $written = @fwrite($this->streamSocket, $msg);
            if ($written === false) {
                throw new \Exception('Error sending data');
            }

            if ($written === 0) {
                throw new \Exception('Broken pipe or closed connection');
            }

            $len = ($len - $written);
            if ($len > 0) {
                $msg = substr($msg, (0 - $len));
            } else {
                break;
            }
        }

        if ($this->debug === true) {
            printf('>>>> %s', $msg);
        }
    }

    /**
     * Receives a message thought the stream.
     *
     * @param integer $len Number of bytes to receive.
     *
     * @return string
     */
    private function receive($len = 0)
    {
        if ($len > 0) {
            $chunkSize     = $this->chunkSize;
            $line          = null;
            $receivedBytes = 0;
            while ($receivedBytes < $len) {
                $bytesLeft = ($len - $receivedBytes);
                if ($bytesLeft < $this->chunkSize) {
                    $chunkSize = $bytesLeft;
                }

                $readChunk      = fread($this->streamSocket, $chunkSize);
                $receivedBytes += strlen($readChunk);
                $line          .= $readChunk;
            }
        } else {
            $line = fgets($this->streamSocket);
        }

        if ($this->debug === true) {
            printf('<<<< %s\r\n', $line);
        }

        return $line;
    }

    /**
     * Handles PING command.
     *
     * @return void
     */
    private function handlePING()
    {
        $this->send('PONG');
    }

    /**
     * Handles MSG command.
     *
     * @param string $line Message command from Nats.
     *
     * @throws             Exception If subscription not found.
     * @return             void
     * @codeCoverageIgnore
     */
    private function handleMSG($line)
    {
        $parts   = explode(' ', $line);
        $subject = null;
        $inbox = null;
        $length  = trim($parts[3]);
        $sid     = $parts[2];

        if (count($parts) === 5) {
            $length  = trim($parts[4]);
            $subject = $parts[1];
            $inbox = $parts[3];
        } elseif (count($parts) === 4) {
            $length  = trim($parts[3]);
            $subject = $parts[1];
        }

        $payload = $this->receive($length);
        $msg     = new Message($subject, $payload, $sid, $this, $inbox);

        if (isset($this->subscriptions[$sid]) === false) {
            throw Exception::forSubscriptionNotFound($sid);
        }

        $func = $this->subscriptions[$sid];
        if (is_callable($func) === true) {
            $func($msg);
        } else {
            throw Exception::forSubscriptionCallbackInvalid($sid);
        }
    }

    /**
     * Connect to server.
     *
     * @param float $timeout Number of seconds until the connect() system call should timeout.
     *
     * @throws \Exception Exception raised if connection fails.
     * @return void
     */
    public function connect($timeout = null)
    {
        if ($timeout === null) {
            $timeout = intval(ini_get('default_socket_timeout'));
        }

        $this->streamSocket = $this->getStream($this->options->getAddress(), $timeout, $this->options->getStreamContext());
        $this->setStreamTimeout($timeout);

        $infoResponse = $this->receive();

        if ($this->isErrorResponse($infoResponse) === true) {
            throw Exception::forFailedConnection($infoResponse);
        } else {
            $this->processServerInfo($infoResponse);
            if ($this->serverInfo->isTLSRequired()) {
                set_error_handler(
                    function ($errno, $errstr, $errfile, $errline) {
                        restore_error_handler();
                        throw Exception::forFailedConnection($errstr);
                    });

                if (!stream_socket_enable_crypto(
                        $this->streamSocket, true, STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT)) {
                    throw Exception::forFailedConnection('Error negotiating crypto');
                }

                restore_error_handler();
            }
        }

        $msg = 'CONNECT '.$this->options;
        $this->send($msg);
        $this->ping();
        $pingResponse = $this->receive();

        if ($this->isErrorResponse($pingResponse) === true) {
            throw Exception::forFailedPing($pingResponse);
        }
    }

    /**
     * Sends PING message.
     *
     * @return void
     */
    public function ping()
    {
        $msg = 'PING';
        $this->send($msg);
        $this->pings += 1;
    }

    /**
     * Request does a request and executes a callback with the response.
     *
     * @param string $subject Message topic.
     * @param string $payload Message data.
     * @param \Closure $callback Closure to be executed as callback.
     * @return void
     */
    public function request($subject, $payload, \Closure $callback)
    {
        $inbox = uniqid('_INBOX.');
        $sid = $this->subscribe($inbox, $callback);
        $this->unsubscribe($sid, 1);
        $this->publish($subject, $payload, $inbox);
        $this->wait(1);
    }

    /**
     * Subscribes to an specific event given a subject.
     *
     * @param string $subject Message topic.
     * @param \Closure $callback Closure to be executed as callback.
     * @return string
     */
    public function subscribe($subject, \Closure $callback)
    {
        $sid = bin2hex(random_bytes(16));
        $msg = 'SUB '.$subject.' '.$sid;
        $this->send($msg);
        $this->subscriptions[$sid] = $callback;
        $this->registeredSubscriptions[$subject] = [null, $callback];
        return $sid;
    }

    /**
     * Subscribes to an specific event given a subject and a queue.
     *
     * @param string $subject Message topic.
     * @param string $queue Queue name.
     * @param \Closure $callback Closure to be executed as callback.
     * @return string
     */
    public function queueSubscribe($subject, $queue, \Closure $callback)
    {
        $sid = bin2hex(random_bytes(16));
        $msg = 'SUB '.$subject.' '.$queue.' '.$sid;
        $this->send($msg);
        $this->subscriptions[$sid] = $callback;
        $this->registeredSubscriptions[$subject] = [$queue, $callback];
        return $sid;
    }

    /**
     * Unsubscribe from a event given a subject.
     *
     * @param string $sid Subscription ID.
     * @param integer $quantity Quantity of messages.
     * @return void
     */
    public function unsubscribe($sid, $quantity = null)
    {
        $msg = 'UNSUB '.$sid;
        if ($quantity !== null) {
            $msg = $msg.' '.$quantity;
        }
        $this->send($msg);
        if ($quantity === null) {
            unset($this->subscriptions[$sid]);
        }
    }

    /**
     * Publish publishes the data argument to the given subject.
     *
     * @param string $subject Message topic.
     * @param string $payload Message data.
     * @param string $inbox Message inbox.
     * @throws Exception If subscription not found.
     * @return void
     *
     */
    public function publish($subject, $payload = null, $inbox = null)
    {
        $msg = 'PUB '.$subject;
        if ($inbox !== null) {
            $msg = $msg.' '.$inbox;
        }

        $msg = $msg.' '.strlen($payload);
        $this->send($msg."\r\n".$payload);
        $this->pubs += 1;
    }

    /**
     * Waits for messages.
     *
     * @param integer $quantity Number of messages to wait for.
     * @return Connection $connection Connection object
     */
    public function wait($quantity = 0)
    {
        $count = 0;
        $info = stream_get_meta_data($this->streamSocket);
        while (is_resource($this->streamSocket) === true && feof($this->streamSocket) === false && empty($info['timed_out']) === true) {
            $line = $this->receive();

            if ($line === false) {
                return null;
            }

            if (strpos($line, 'PING') === 0) {
                $this->handlePING();
            }

            if (strpos($line, 'MSG') === 0) {
                $count++;
                $this->handleMSG($line);
                if (($quantity !== 0) && ($count >= $quantity)) {
                    return $this;
                }
            }

            if (!$this->isConnected()) {
                break;
            }
            $info = stream_get_meta_data($this->streamSocket);
        }

        $this->close();

        return $this;
    }

    /**
     * Reconnects to the server.
     *
     * @return void
     */
    public function reconnect($resubscribe = false)
    {
        $sids = [];
        $this->reconnects += 1;
        $this->close();
        $this->connect($this->timeout);
        if ($resubscribe) {
            if ($this->isConnected()) {
                foreach ($this->registeredSubscriptions as $subject => $info) {
                    if ($info[0] === null) {
                        $sids[$subject] = $this->subscribe($subject, $info[1]);
                    }
                    else {
                        $sids[$subject] = $this->queueSubscribe($subject, $info[0], $info[1]);
                    }
                }
            }
        }
        return $sids;
    }

    /**
     * Close will close the connection to the server.
     *
     * @return void
     */
    public function close()
    {
        if ($this->streamSocket === null) {
            return;
        }
        fclose($this->streamSocket);
        $this->streamSocket = null;
    }
    
    /**
     * Returns array containing seconds and miliseconds
     *
     * @author ikubicki
     * @param float $timeout
     * @return array
     */
    public function getNumberAndDecimals($timeout)
    {
        return [
            intval($timeout),
            intval(($timeout % 1) * 1000)
        ];
    }
}
